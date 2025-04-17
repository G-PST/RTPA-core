// This code is for creating an http server for end users
//
// It should start a pdc_client that buffers data frames from an
// upstream pdc server.
//
// It should allow users to request the entire buffer,
// or last N seconds of buffer data.
//
// It should parse the buffer data into the a dataframe based
// on the IEEE C37.118.2 standard.
//
// It should return that data as arrow dataframes over IPC.
//
// What it shouldn't do. (for now)
// Send configuration commands to the upstream pdc server.
//
//#![allow(unused)]
use crate::arrow_utils::{build_arrow_schema, extract_channel_values};
use crate::frames::{ChannelInfo, ConfigurationFrame1and2_2011};
use crate::pdc_client::{ControlMessage, PDCClient};
use arrow::array::ArrayRef;
use arrow::array::TimestampMicrosecondArray;
use arrow::array::{Float32Array, Int16Array, UInt16Array};
use arrow::datatypes::DataType;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use axum::extract::{Json as JsonForm, Query, State};
use axum::{http::StatusCode, response::IntoResponse, routing::get, Json, Router};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tracing::{error, info, warn};
// Connection status of the pdc server.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionStatus {
    Connected,
    Disconnected,
    Reconnecting,
    Failed,
}
// Basic configuration structure
#[derive(Debug, Clone)]
struct Config {
    pdc_host: String,
    pdc_port: u16,
    pdc_idcode: u16,
    buffer_duration: Duration,
    server_port: u16,
    max_reconnection_attempts: u32,
    reconnection_delay: Duration,
}

impl Config {
    fn from_env() -> Result<Self, String> {
        Ok(Config {
            pdc_host: env::var("PDC_HOST").unwrap_or_else(|_| "localhost".to_string()),
            pdc_port: env::var("PDC_PORT")
                .unwrap_or_else(|_| "8123".to_string())
                .parse()
                .map_err(|_| "Invalid PDC_PORT")?,
            pdc_idcode: env::var("PDC_IDCODE")
                .unwrap_or_else(|_| "1".to_string())
                .parse()
                .map_err(|_| "Invalid PDC_IDCODE")?,
            buffer_duration: Duration::from_secs(
                env::var("BUFFER_DURATION_SECS")
                    .unwrap_or_else(|_| "120".to_string())
                    .parse()
                    .map_err(|_| "Invalid BUFFER_DURATION_SECS")?,
            ),
            server_port: env::var("SERVER_PORT")
                .unwrap_or_else(|_| "8080".to_string())
                .parse()
                .map_err(|_| "Invalid SERVER_PORT")?,
            max_reconnection_attempts: env::var("MAX_RECONNECTION_ATTEMPTS")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .map_err(|_| "Invalid MAX_RECONNECTION_ATTEMPTS")?,
            reconnection_delay: Duration::from_secs(
                env::var("RECONNECTION_DELAY_SECS")
                    .unwrap_or_else(|_| "30".to_string())
                    .parse()
                    .map_err(|_| "Invalid RECONNECTION_DELAY_SECS")?,
            ),
        })
    }
}

// Application state
#[derive(Clone)]
struct AppState {
    control_tx: Arc<Mutex<mpsc::Sender<ControlMessage>>>,
    //data_rx: mpsc::Receiver<Vec<u8>>,
    config: ConfigurationFrame1and2_2011,
    frame_size: usize,
    data_rx: Arc<Mutex<mpsc::Receiver<Vec<u8>>>>,
    // Pre-parsed channel map. Tells you index of channel value in a data frame.
    channel_map: Arc<HashMap<String, ChannelInfo>>,
    // pre-made schema, so a full schema doesn't need to be made.
    full_schema: Arc<arrow::datatypes::Schema>,
    connection_status: Arc<Mutex<ConnectionStatus>>,
    reconnection_attempts: Arc<AtomicU32>,
}

async fn handle_reconnection(
    config: Config,
    connection_status: Arc<Mutex<ConnectionStatus>>,
    reconnection_attempts: Arc<AtomicU32>,
    pdc_client: &mut PDCClient,
) -> Option<()> {
    while reconnection_attempts.load(Ordering::SeqCst) < config.max_reconnection_attempts {
        {
            let mut status = connection_status.lock().await;
            *status = ConnectionStatus::Reconnecting;
        }

        info!("Attempting to reconnect to PDC server...");

        match pdc_client
            .reconnect(&config.pdc_host, config.pdc_port, config.pdc_idcode)
            .await
        {
            Ok(()) => {
                {
                    let mut status = connection_status.lock().await;
                    *status = ConnectionStatus::Connected;
                }
                reconnection_attempts.store(0, Ordering::SeqCst);
                info!("Successfully reconnected to PDC server");
                return Some(());
            }
            Err(e) => {
                error!("Reconnection attempt failed: {}", e);
                reconnection_attempts.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(config.reconnection_delay).await;
            }
        }
    }

    {
        let mut status = connection_status.lock().await;
        *status = ConnectionStatus::Failed;
    }
    error!("Max reconnection attempts reached");
    None
}
//// ROUTE Specific ////
#[derive(Debug, Deserialize)]
struct TimeRangeParams {
    #[serde(default)]
    start_time: Option<i64>, // Unix timestamp in microseconds
    #[serde(default)]
    end_time: Option<i64>, // Unix timestamp in microseconds
}

#[derive(Debug, Deserialize, Clone)]
struct ColumnSelection {
    columns: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    message: String,
    invalid_columns: Vec<String>,
}

// Define a concrete response type
type DataResponse = (
    StatusCode,
    [(axum::http::header::HeaderName, &'static str); 1],
    Bytes,
);

async fn get_health(State(state): State<AppState>) -> Json<serde_json::Value> {
    let status = state.connection_status.lock().await.clone();

    Json(serde_json::json!({
        "status": status,
        "healthy": matches!(status, ConnectionStatus::Connected)
    }))
}
async fn get_version() -> Json<serde_json::Value> {
    // Return some test configuration
    Json(serde_json::json!({
        "software_name": env!("CARGO_PKG_NAME"),
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

async fn get_config(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.config)
}
async fn get_status(State(state): State<AppState>) -> impl IntoResponse {
    let status = state.connection_status.lock().await.clone();
    Json(json!({
        "status": status,
        "reconnection_attempts": state.reconnection_attempts.load(Ordering::SeqCst)
    }))
}
// Split into two separate handler functions
async fn get_data_all(
    State(state): State<AppState>,
    Query(time_params): Query<TimeRangeParams>,
) -> Result<DataResponse, (StatusCode, Json<ErrorResponse>)> {
    info!("Received request for all data");
    process_data_request(state, time_params, None).await
}

async fn get_data_filtered(
    State(state): State<AppState>,
    Query(time_params): Query<TimeRangeParams>,
    JsonForm(column_selection): JsonForm<ColumnSelection>,
) -> Result<DataResponse, (StatusCode, Json<ErrorResponse>)> {
    info!("Received request for filtered data");
    process_data_request(state, time_params, Some(column_selection)).await
}

async fn get_phasors_data(
    State(state): State<AppState>,
    Query(time_params): Query<TimeRangeParams>,
) -> Result<DataResponse, (StatusCode, Json<ErrorResponse>)> {
    info!("Received request for phasor data");

    let phasor_columns = state
        .config
        .pmu_configs
        .iter()
        .flat_map(|config| config.get_phasor_columns())
        .collect();

    let column_selection = ColumnSelection {
        columns: phasor_columns,
    };

    process_data_request(state, time_params, Some(column_selection)).await
}

async fn get_analog_data(
    State(state): State<AppState>,
    Query(time_params): Query<TimeRangeParams>,
) -> Result<DataResponse, (StatusCode, Json<ErrorResponse>)> {
    info!("Received request for analog data");

    let analog_columns = state
        .config
        .pmu_configs
        .iter()
        .flat_map(|config| config.get_analog_columns())
        .collect();

    let column_selection = ColumnSelection {
        columns: analog_columns,
    };

    process_data_request(state, time_params, Some(column_selection)).await
}

async fn get_digital_data(
    State(state): State<AppState>,
    Query(time_params): Query<TimeRangeParams>,
) -> Result<DataResponse, (StatusCode, Json<ErrorResponse>)> {
    info!("Received request for digital data");

    let digital_columns = state
        .config
        .pmu_configs
        .iter()
        .flat_map(|config| config.get_digital_columns())
        .collect();

    let column_selection = ColumnSelection {
        columns: digital_columns,
    };

    process_data_request(state, time_params, Some(column_selection)).await
}

// Common processing function

async fn process_data_request(
    state: AppState,
    time_params: TimeRangeParams,
    column_selection: Option<ColumnSelection>,
    //data_rx: Arc<Mutex<mpsc::Receiver<Vec<u8>>>>,
) -> Result<DataResponse, (StatusCode, Json<ErrorResponse>)> {
    let start = Instant::now();

    info!(
        "Processing request - time_params: {:?}, has_column_selection: {}",
        time_params,
        column_selection.is_some()
    );

    // Use schema and channel map based on selection
    let (schema, working_channel_map) = if let Some(selection) = &column_selection {
        // Create filtered channel map without cloning the entire original map
        let filtered_map: HashMap<String, ChannelInfo> = selection
            .columns
            .iter()
            .filter_map(|name| {
                state
                    .channel_map
                    .get(name)
                    .map(|info| (name.clone(), info.clone()))
            })
            .collect();

        // Only build new schema if we're using a subset of channels
        (Arc::new(build_arrow_schema(&filtered_map)), filtered_map)
    } else {
        // Use pre-built full schema and channel map
        (
            state.full_schema.clone(),
            state.channel_map.as_ref().clone(),
        )
    };
    // Send request for buffer

    state
        .control_tx
        .lock()
        .await
        .send(ControlMessage::GetBuffer)
        .await
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    message: "Failed to request buffer".to_string(),
                    invalid_columns: vec![],
                }),
            )
        })?;

    // Wait for response
    let buffer = {
        let mut rx = state.data_rx.lock().await;
        tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .map_err(|_| {
                error!("Request Timeout");
                (
                    StatusCode::REQUEST_TIMEOUT,
                    Json(ErrorResponse {
                        message: "Request timeout".to_string(),
                        invalid_columns: vec![],
                    }),
                )
            })?
            .ok_or_else(|| {
                error!("Failed to receive buffer");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        message: "Failed to receive buffer".to_string(),
                        invalid_columns: vec![],
                    }),
                )
            })?
    };
    info!(
        "Retrieved buffer with {} frames",
        buffer.len() / state.frame_size
    );

    // Create arrays for selected columns
    let mut arrays: Vec<ArrayRef> = Vec::new();

    // Process timestamps and create time filter mask
    let mut timestamps = Vec::new();
    let mut time_mask = Vec::new();

    for frame in buffer.chunks(state.frame_size) {
        if frame.len() >= 14 {
            let soc = u32::from_be_bytes([frame[6], frame[7], frame[8], frame[9]]);
            let fracsec = u32::from_be_bytes([frame[10], frame[11], frame[12], frame[13]]);
            let ts = (soc as i64) * 1_000_000 + (fracsec as i64);

            // Apply time filtering
            let include_row = match (time_params.start_time, time_params.end_time) {
                (Some(start), Some(end)) => ts >= start && ts <= end,
                (Some(start), None) => ts >= start,
                (None, Some(end)) => ts <= end,
                (None, None) => true,
            };

            time_mask.push(include_row);
            if include_row {
                timestamps.push(ts);
            }
        }
    }

    // Only proceed if we have data after filtering
    if timestamps.is_empty() {
        warn!(
            "No data found in specified time range: {:?} to {:?}",
            time_params.start_time, time_params.end_time
        );
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                message: "No data found in the specified time range".to_string(),
                invalid_columns: vec![],
            }),
        ));
    }

    arrays.push(Arc::new(TimestampMicrosecondArray::from(timestamps)));
    // Extract values for selected channels
    for (_name, info) in &working_channel_map {
        let channel_arrays = extract_channel_values(&buffer, state.frame_size, info);
        for array in channel_arrays {
            let filtered_array = filter_array_by_mask(&array, &time_mask);
            arrays.push(filtered_array);
        }
    }

    // Create RecordBatch
    let record_batch = RecordBatch::try_new(schema.clone(), arrays).map_err(|e| {
        let err_msg = format!("Failed to create record batch: {}", e);
        error!(err_msg);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                message: err_msg,
                invalid_columns: vec![],
            }),
        )
    })?;

    // Serialize to Arrow IPC format
    let mut buf = Vec::new();
    {
        let mut writer = FileWriter::try_new(&mut buf, &schema).map_err(|e| {
            let err_msg = format!("Failed to create Arrow writer: {}", e);
            error!(err_msg);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    message: err_msg,
                    invalid_columns: vec![],
                }),
            )
        })?;

        writer.write(&record_batch).map_err(|e| {
            let err_msg = format!("Failed to write record batch: {}", e);
            error!(err_msg);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    message: err_msg,
                    invalid_columns: vec![],
                }),
            )
        })?;

        writer.finish().map_err(|e| {
            let err_msg = format!("Failed to finish Arrow writer: {}", e);
            error!(err_msg);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    message: err_msg,
                    invalid_columns: vec![],
                }),
            )
        })?;
    }

    info!(
        "Total request processing took {} ms",
        start.elapsed().as_millis()
    );

    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/arrow-ipc")],
        Bytes::from(buf),
    ))
}

fn filter_array_by_mask(array: &ArrayRef, mask: &[bool]) -> ArrayRef {
    match array.data_type() {
        DataType::Int16 => {
            let array = array.as_any().downcast_ref::<Int16Array>().unwrap();
            Arc::new(
                array
                    .iter()
                    .zip(mask.iter())
                    .filter_map(|(v, &m)| if m { v } else { None })
                    .collect::<Int16Array>(),
            )
        }
        DataType::Float32 => {
            let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
            Arc::new(
                array
                    .iter()
                    .zip(mask.iter())
                    .filter_map(|(v, &m)| if m { v } else { None })
                    .collect::<Float32Array>(),
            )
        }
        DataType::UInt16 => {
            let array = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            Arc::new(
                array
                    .iter()
                    .zip(mask.iter())
                    .filter_map(|(v, &m)| if m { v } else { None })
                    .collect::<UInt16Array>(),
            )
        }

        // Add other data types as needed
        _ => {
            warn!(
                "Unsupported data type for filtering: {:?}",
                array.data_type()
            );
            array.clone()
        }
    }
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    // Setup logging
    std::fs::create_dir_all("./logs").map_err(|e| {
        error!("Failed to create logs directory: {}", e);
        e
    })?;
    let file_appender = tracing_appender::rolling::hourly("./logs", "pdc_server.log");

    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_writer(non_blocking)
        .with_target(false)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .init();

    info!("Starting PDC buffer server");

    // Load configuration
    let config = Config::from_env()?;

    let connection_status = Arc::new(Mutex::new(ConnectionStatus::Connected));
    let reconnection_attempts = Arc::new(AtomicU32::new(0));

    let connection_status_clone = connection_status.clone();
    let reconnection_attempts_clone = reconnection_attempts.clone();
    let config_clone = config.clone();

    // Initialize PDC client
    let (mut pdc_client, control_tx, data_rx) = match PDCClient::new(
        &config.pdc_host,
        config.pdc_port,
        config.pdc_idcode,
        config.buffer_duration,
    )
    .await
    {
        Ok(client_tuple) => client_tuple,
        Err(e) => {
            error!("Failed to connect to PDC Server: {}", e);
            eprintln!("Failed to connect to PDC server: {}", e);
            // Exit the process with error code
            std::process::exit(1);
        }
    };
    info!("Connected to PDC Server");
    // Start PDC client in background
    // Get initial configuration

    let frame_size = pdc_client.frame_size;
    let pdc_config = pdc_client
        .get_config()
        .expect("No Configuration Frame, shutting down.");

    // Parse channel map once
    let channel_map = Arc::new(pdc_config.get_channel_map());

    // Build full schema once
    let full_schema = Arc::new(build_arrow_schema(&channel_map));

    // Create application state
    let app_state = AppState {
        control_tx: Arc::new(Mutex::new(control_tx)),
        config: pdc_config,
        frame_size,
        data_rx: Arc::new(Mutex::new(data_rx)),
        channel_map,
        full_schema,
        connection_status,
        reconnection_attempts,
    };
    let _client_handle = tokio::spawn(async move {
        loop {
            info!("PDC client stream started");
            pdc_client.start_stream().await;
            error!("PDC client stream ended unexpectedly");

            {
                let mut status = connection_status_clone.lock().await;
                *status = ConnectionStatus::Disconnected;
            }

            // Attempt reconnection
            match handle_reconnection(
                config_clone.clone(),
                connection_status_clone.clone(),
                reconnection_attempts_clone.clone(),
                &mut pdc_client,
            )
            .await
            {
                Some(()) => {
                    info!("Successfully reconnected");
                }
                None => {
                    error!("Failed to reconnect to PDC server after max attempts");
                    std::process::exit(1);
                }
            }
        }
    });
    // Set up Routes
    let app = Router::new()
        .route("/health", get(get_health))
        .route("/version", get(get_version))
        .route("/config", get(get_config))
        .route("/status", get(get_status))
        .route("/data", get(get_data_all).post(get_data_filtered))
        .route("/data/phasors", get(get_phasors_data))
        .route("/data/analogs", get(get_analog_data))
        .route("/data/digitals", get(get_digital_data))
        .with_state(app_state);

    // Start server
    let addr = SocketAddr::from(([0, 0, 0, 0], config.server_port));
    println!("Server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    println!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();

    Ok(())
}
