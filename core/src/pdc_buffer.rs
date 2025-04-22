//! # IEEE C37.118 PDC Server Interface
//!
//! This module provides `PDCBuffer`, a centralized interface for interacting with a Phasor
//! Data Concentrator (PDC) server over TCP, as defined in IEEE C37.118-2005,
//! IEEE C37.118.2-2011, and IEEE C37.118.2-2024 standards. It manages connections, sends
//! command frames, parses configuration frames, and processes data streams into Arrow
//! record batches using a producer-consumer model.
//!
//! ## Key Components
//!
//! - `PDCBuffer`: Manages the TCP connection, configuration, and data streaming with an
//!   `AccumulatorManager` for timeseries processing.
//!
//! ## Usage
//!
//! This module is used to connect to a PDC server, request configuration, start/stop data
//! streams, and retrieve timeseries data as Arrow record batches. It integrates with the
//! `accumulator::manager` for data processing, `commands` for frame creation, `common` for
//! shared types, `config` for configuration parsing, `phasors` for phasor handling, and
//! `utils` for checksum validation, suitable for power system monitoring applications.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::mpsc::{self, Receiver, Sender, SyncSender};
use std::sync::{Arc, Mutex};

use std::thread::{self, JoinHandle};

use arrow::record_batch::RecordBatch;

use crate::accumulator::manager::{AccumulatorConfig, AccumulatorManager};

use crate::ieee_c37_118::commands::CommandFrame;
use crate::ieee_c37_118::common::{FrameType, Version};
use crate::ieee_c37_118::config::ConfigurationFrame;
use crate::ieee_c37_118::phasors::PhasorType;
use crate::ieee_c37_118::utils::validate_checksum;
use crate::utils::config_to_accumulators;

const DEFAULT_STANDARD: Version = Version::V2011;
const DEFAULT_MAX_BATCHES: usize = 120; // ~4 minutes at 60hz
const DEFAULT_BATCH_SIZE: usize = 128; // ~2 seconds of data at 60hz
const DEFAULT_TIMEOUT: u64 = 5; // 5 seconds timeout

/// Interface for interacting with a PDC server over TCP for IEEE C37.118 data.
///
/// This struct manages the connection to a PDC server, sends command frames, parses
/// configuration frames, and processes data streams into Arrow record batches using a
/// producer-consumer model with background threads for streaming.
///
/// # Fields
///
/// * `ip_addr`: IP address of the PDC server.
/// * `port`: TCP port of the PDC server.
/// * `id_code`: Identification code for communication with the PDC.
/// * `_accumulators`: Configurations for regular accumulators.
/// * `_accumulator_manager`: Thread-safe manager for processing data into record batches.
/// * `ieee_version`: IEEE C37.118 standard version (e.g., 2011).
/// * `config_frame`: Parsed configuration frame from the PDC.
/// * `_data_frame_size`: Expected size of data frames.
/// * `_batch_size`: Number of buffers per batch.
/// * `_channels`: List of channel names (currently unimplemented).
/// * `_pmus`: List of PMU names (currently unimplemented).
/// * `_stations`: List of station names (currently unimplemented).
/// * `producer_handle`: Handle for the producer thread (optional).
/// * `consumer_handle`: Handle for the consumer thread (optional).
/// * `shutdown_tx`: Channel for signaling shutdown (optional).
/// * `latest_buffer_request_tx`: Channel for requesting the latest data buffer.
pub struct PDCBuffer {
    pub ip_addr: String,
    pub port: u16,
    pub id_code: u16,
    _accumulators: Vec<AccumulatorConfig>,
    _accumulator_manager: Arc<Mutex<AccumulatorManager>>, // Thread safe manager.
    pub ieee_version: Version,                            // The ieee standard version 1&2 or 3
    pub config_frame: ConfigurationFrame,
    _data_frame_size: usize,
    _batch_size: usize,
    _channels: Vec<String>,
    _pmus: Vec<String>,
    _stations: Vec<String>,
    producer_handle: Option<JoinHandle<()>>,
    consumer_handle: Option<JoinHandle<()>>,
    shutdown_tx: Option<Sender<()>>,
    latest_buffer_request_tx: Arc<Mutex<Option<mpsc::Sender<mpsc::Sender<Vec<u8>>>>>>,
}

impl PDCBuffer {
    /// Creates a new PDC buffer and initializes the connection.
    ///
    /// Connects to the PDC server, requests the configuration frame, and sets up the
    /// accumulator manager based on the configuration.
    ///
    /// # Parameters
    ///
    /// * `ip_addr`: IP address of the PDC server.
    /// * `port`: TCP port of the PDC server.
    /// * `id_code`: Identification code for communication.
    /// * `version`: Optional IEEE C37.118 version (defaults to 2011).
    /// * `batch_size`: Optional number of buffers per batch (defaults to 128).
    /// * `max_batches`: Optional maximum number of batches to retain (defaults to 120).
    /// * `output_phasor_type`: Optional output format for phasors (defaults to native).
    ///
    /// # Returns
    ///
    /// A new `PDCBuffer` instance.
    ///
    /// # Panics
    ///
    /// Panics if the connection fails, the configuration frame is invalid, or the frame
    /// size exceeds 65KB.
    pub fn new(
        ip_addr: String,
        port: u16,
        id_code: u16,
        version: Option<Version>,
        batch_size: Option<usize>,
        max_batches: Option<usize>,
        output_phasor_type: Option<PhasorType>,
    ) -> Self {
        // try to connect to the tcp socket.
        //
        // If connected, send command frame to request the PDC configuration and header.
        //
        // If successful,
        // 1. parse the configuration frame and determine buffer size. (How long each data frame will be)
        // 2. Parse the configuration and generate SparseAccumulators and Accumulator Configs for the Accumulator Manager..
        // 3. Based on the information in the configuration frame, also set the maximum buffer size of the Accumulator Manager.
        // and the producer/consumer queue buffer. Fixed size as a multiple of the pdc buffer size.
        // 4. Sets the batch size based on frequency found in the configuration frame.

        let mut stream = TcpStream::connect(format!("{}:{}", ip_addr, port)).unwrap();

        // Set read and write timeouts
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(DEFAULT_TIMEOUT)))
            .unwrap();
        stream
            .set_write_timeout(Some(std::time::Duration::from_secs(DEFAULT_TIMEOUT)))
            .unwrap();

        // default to the 2011 standard.
        // Use the methods for CommandFrame to generate a CommandFrame to request data.
        // TODO we need to implement a function to create the command frame based on version desired.
        // Default to version 1 and 2 for now.
        //
        let send_config_cmd = CommandFrame::new_send_config_frame2(id_code, None);

        stream.write_all(&send_config_cmd.to_hex()).unwrap();

        // Read the SYNC and Frame Size of the configuration frame.
        let mut peek_buffer: [u8; 4] = [0u8; 4];
        stream.read_exact(&mut peek_buffer).unwrap();

        let sync = u16::from_be_bytes([peek_buffer[0], peek_buffer[1]]);

        let frame_type: FrameType = FrameType::from_sync(sync).unwrap();
        println!("Detected Frame: {}", frame_type);

        let detected_version: Version = Version::from_sync(sync).unwrap();
        println!("Version: {}", detected_version);

        let frame_size = u16::from_be_bytes([peek_buffer[2], peek_buffer[3]]);
        // ensure frame size is less than 65 kB
        if frame_size > u16::MAX {
            panic!("Frame size exceeds maximum allowable limit of 65535 bytes!")
        }

        let remaining_size = frame_size as usize - 4;

        let mut remaining_buffer = vec![0u8; remaining_size];

        // read the entire buffer up to frame_size
        stream.read_exact(&mut remaining_buffer).unwrap();

        // Combine the peek and remaining buffers
        let mut buffer = Vec::with_capacity(frame_size as usize);
        buffer.extend_from_slice(&peek_buffer); // Add the first 4 bytes
        buffer.extend_from_slice(&remaining_buffer); // Add the rest

        // Parse the bytes into the ConfigurationFrame struct, depending on the version.
        //
        //

        let config_frame: ConfigurationFrame = ConfigurationFrame::from_hex(&buffer).unwrap();

        // Determine data frame size expected by the configuration frame
        let data_frame_size = config_frame.calc_data_frame_size();

        if output_phasor_type.is_none() {
            println!("Using native phasor values. Users will need to utilize scale factors in the
                configuration frame to properly scale the integer phasor values if present.
                Use a FloatRect or FloatPolar output format to automatically scale the phasor values.
                See IEEE C37.118 Documentation for more details.");
        }

        let (accumulators, phasor_accumulators) =
            config_to_accumulators(&config_frame, output_phasor_type, None);

        let accumulator_manager = AccumulatorManager::new_with_params(
            accumulators.clone(),
            phasor_accumulators.clone(),
            max_batches.unwrap_or(DEFAULT_MAX_BATCHES),
            data_frame_size,
            batch_size.unwrap_or(DEFAULT_BATCH_SIZE),
        );

        println!("RTPA Buffer Initialization Successful");
        PDCBuffer {
            ip_addr,
            port,
            id_code,
            _accumulators: accumulators,
            _accumulator_manager: Arc::new(Mutex::new(accumulator_manager)),
            ieee_version: version.unwrap_or(DEFAULT_STANDARD),
            config_frame,
            _data_frame_size: data_frame_size,
            _batch_size: 120,
            _channels: vec![],
            _pmus: vec![],
            _stations: vec![],
            producer_handle: None,
            consumer_handle: None,
            shutdown_tx: None,
            latest_buffer_request_tx: Arc::new(Mutex::new(None)),
        }
    }

    /// Starts streaming data from the PDC server.
    ///
    /// Initiates a producer-consumer model with background threads: the producer sends a
    /// start command and reads data frames, while the consumer validates and processes them
    /// using the accumulator manager.
    ///
    /// # Panics
    ///
    /// Panics if the connection fails.
    pub fn start_stream(&mut self) {
        // Start the stream
        //
        // Producer consumer model running in background threads. Not main thread.
        //
        // Producer thread connects to the PDC server.
        //
        // Producer thread sends the command frame to start the stream.
        //
        // Producer threads appends frame to a queue.
        //
        // Consumer thread reads a frame from the queue and performs crc check.
        //
        // If crc check passes, the frame is processed by the Accumulator Manager.
        //
        let stream = TcpStream::connect(format!("{}:{}", self.ip_addr, self.port))
            .expect("Failed to connect");

        // Set read and write timeouts
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(DEFAULT_TIMEOUT)))
            .expect("Failed to set TCP read timeout");

        stream
            .set_write_timeout(Some(std::time::Duration::from_secs(DEFAULT_TIMEOUT)))
            .expect("Failed to set TCP write timeout");

        let (tx, rx): (SyncSender<Vec<u8>>, Receiver<Vec<u8>>) = mpsc::sync_channel(100);
        let (shutdown_tx, shutdown_rx) = mpsc::channel();

        let consumer_manager = self._accumulator_manager.clone();
        let start_stream_cmd = CommandFrame::new_turn_on_transmission(self.id_code, None);

        let (buffer_request_tx, buffer_request_rx) = mpsc::channel();

        // Store the sender in our struct
        if let Ok(mut tx_guard) = self.latest_buffer_request_tx.lock() {
            *tx_guard = Some(buffer_request_tx);
        }

        let data_frame_size = self._data_frame_size;

        let producer = {
            let mut stream = stream.try_clone().unwrap();
            thread::spawn(move || {
                if stream.write_all(&start_stream_cmd.to_hex()).is_err() {
                    println!("Failed to send START_STREAM command");
                    return;
                }

                let mut peek_buffer = [0u8; 4];
                // TODO we should know the size of this frame after reading the configuration.
                let mut current_frame_buffer = vec![0u8; data_frame_size];

                loop {
                    if let Ok(response_tx) = buffer_request_rx.try_recv() {
                        // Send the latest frame buffer through the one-shot channel
                        let _ = response_tx.send(current_frame_buffer.clone());
                    }

                    if shutdown_rx.try_recv().is_ok() {
                        println!("Producer shutting down");
                        break;
                    }

                    // Read SYNC and frame size (4 bytes)
                    match stream.read_exact(&mut peek_buffer) {
                        Ok(()) => {
                            let frame_size =
                                u16::from_be_bytes([peek_buffer[2], peek_buffer[3]]) as usize;
                            if frame_size != data_frame_size {
                                println!(
                                    "Unexpected frame size: {}. Expected {}",
                                    frame_size, data_frame_size
                                );
                                continue;
                            }

                            // Read the rest of the frame
                            current_frame_buffer = vec![0u8; frame_size];
                            current_frame_buffer[..4].copy_from_slice(&peek_buffer); // Include SYNC and size

                            if frame_size > 4 {
                                match stream.read_exact(&mut current_frame_buffer[4..]) {
                                    Ok(()) => {
                                        if tx.send(current_frame_buffer.clone()).is_err() {
                                            println!("Consumer disconnected");
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        println!("Stream read error: {}", e);
                                        break;
                                    }
                                }
                            } else {
                                if tx.send(current_frame_buffer.clone()).is_err() {
                                    println!("Consumer disconnected");
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            println!("Stream read error: {}", e);
                            break;
                        }
                    }
                }
            })
        };

        let consumer = {
            thread::spawn(move || {
                while let Ok(frame) = rx.recv() {
                    match validate_checksum(&frame) {
                        Ok(()) => (),
                        Err(_) => {
                            println!("Invalid Checksum, Skipping buffer.")
                        }
                    }

                    // Lock the manager to process the buffer
                    let mut manager = match consumer_manager.lock() {
                        Ok(manager) => manager,
                        Err(e) => {
                            println!("Failed to lock accumulator manager: {:?}", e);
                            continue;
                        }
                    };

                    if let Err(e) = manager.process_buffer(&frame) {
                        println!("Error processing frame: {}", e);
                    }
                }
                println!("Consumer shutting down");
            })
        };

        self.producer_handle = Some(producer);
        self.consumer_handle = Some(consumer);
        self.shutdown_tx = Some(shutdown_tx);
    }

    /// Stops streaming data from the PDC server.
    ///
    /// Sends a stop command, signals the producer and consumer threads to shut down, and
    /// closes the TCP connection.
    pub fn stop_stream(&mut self) {
        println!("Stopping PDC stream");

        // Clear the buffer request channel
        if let Ok(mut tx_guard) = self.latest_buffer_request_tx.lock() {
            *tx_guard = None;
        }

        if let Ok(mut stream) = TcpStream::connect(format!("{}:{}", self.ip_addr, self.port)) {
            let stop_stream_cmd = CommandFrame::new_turn_off_transmission(self.id_code, None);
            let _ = stream.write_all(&stop_stream_cmd.to_hex());
            let _ = stream.shutdown(std::net::Shutdown::Both);
        }

        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }

        if let Some(handle) = self.producer_handle.take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.consumer_handle.take() {
            let _ = handle.join();
        }
    }

    /// Converts the configuration frame to JSON.
    ///
    /// # Returns
    ///
    /// * `Ok(String)`: JSON representation of the configuration frame.
    /// * `Err(serde_json::Error)`: If serialization fails.
    pub fn config_to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&self.config_frame)
    }

    /// Retrieves the PDC header frame.
    ///
    /// # Note
    ///
    /// Currently unimplemented; intended to return the header frame.
    pub fn get_pdc_header(&self) {
        // Get the header
        // TODO: implement the header frame.
        todo!()
    }

    /// Resets the accumulator configuration to filter by a specified set of PMU ID codes or all PMUs.
    ///
    /// Stops any active streaming, creates new accumulator configurations filtered by the provided
    /// `id_codes`, and replaces the accumulator manager with a new one, resetting all internal data.
    /// If `id_codes` is `None`, accumulators for all PMUs are included.
    ///
    /// # Parameters
    ///
    /// * `id_codes`: Optional list of PMU ID codes to filter accumulators (all PMUs if `None`).
    /// * `output_phasor_type`: Optional output format for phasors (defaults to native).
    ///
    /// # Returns
    ///
    /// * `Ok(())`: If the reset is successful.
    /// * `Err(String)`: If the accumulator manager cannot be locked or initialized.
    pub fn set_pmu_filter(
        &mut self,
        id_codes: Option<Vec<u16>>,
        output_phasor_type: Option<PhasorType>,
    ) -> Result<(), String> {
        // Stop any active streaming to ensure thread safety
        self.stop_stream();

        // Generate new accumulator configurations
        let (new_accumulators, new_phasor_accumulators) =
            config_to_accumulators(&self.config_frame, output_phasor_type, id_codes);

        // Create a new AccumulatorManager
        let new_manager = AccumulatorManager::new_with_params(
            new_accumulators.clone(),
            new_phasor_accumulators,
            DEFAULT_MAX_BATCHES,
            self._data_frame_size,
            self._batch_size,
        );

        // Update the accumulators and manager
        self._accumulators = new_accumulators;
        let mut manager = self
            ._accumulator_manager
            .lock()
            .map_err(|e| format!("Failed to lock accumulator manager: {}", e))?;
        *manager = new_manager;

        println!("Accumulator configuration reset successfully");
        Ok(())
    }

    /// Lists available PMU station names and their corresponding ID codes.
    ///
    /// # Returns
    ///
    /// A vector of tuples containing `(station_name, id_code)` for each PMU in the configuration.
    pub fn list_pmus(&self) -> Vec<(String, u16)> {
        self.config_frame
            .pmu_configs
            .iter()
            .map(|pmu| {
                (
                    String::from_utf8_lossy(&pmu.stn).trim().to_string(),
                    pmu.idcode,
                )
            })
            .collect()
    }

    /// Retrieves timeseries data as an Arrow record batch.
    ///
    /// # Parameters
    ///
    /// * `columns`: Optional list of column names to include (all columns if `None`).
    /// * `window_secs`: Optional time window in seconds (all data if `None`).
    ///
    /// # Returns
    ///
    /// * `Ok(RecordBatch)`: The requested timeseries data.
    /// * `Err(String)`: If the accumulator manager is locked or data retrieval fails.
    pub fn get_data(
        &self,
        columns: Option<Vec<&str>>, // Optional list of column names
        window_secs: Option<u64>,
    ) -> Result<RecordBatch, String> {
        // Get a mutable reference to the manager and get dataframe
        let mut manager = match self._accumulator_manager.lock() {
            Ok(manager) => manager,
            Err(e) => return Err(format!("Failed to lock accumulator manager: {:?}", e)),
        };

        manager
            .get_dataframe(columns, window_secs)
            .map_err(|e| format!("Failed to get dataframes: {:?}", e))
    }

    /// Retrieves the latest data frame buffer.
    ///
    /// # Returns
    ///
    /// * `Ok(Vec<u8>)`: The latest data frame buffer.
    /// * `Err(String)`: If the stream is not started, no data is available, or the request times out.
    pub fn get_latest_buffer(&self) -> Result<Vec<u8>, String> {
        // Create a one-shot channel for the response
        let (response_tx, response_rx) = mpsc::channel();

        // Send the request
        let request_sent = {
            if let Ok(tx_guard) = self.latest_buffer_request_tx.lock() {
                if let Some(tx) = tx_guard.as_ref() {
                    tx.send(response_tx).is_ok()
                } else {
                    false
                }
            } else {
                false
            }
        };

        if !request_sent {
            return Err("Stream not started or request channel not available".to_string());
        }

        // Wait for the response with a timeout
        match response_rx.recv_timeout(std::time::Duration::from_secs(1)) {
            Ok(buffer) => {
                if buffer.is_empty() {
                    Err("No data frames available yet".to_string())
                } else {
                    Ok(buffer)
                }
            }
            Err(_) => Err("Timeout waiting for latest buffer".to_string()),
        }
    }

    /// Retrieves the location and length of a channel in the input buffer.
    ///
    /// # Parameters
    ///
    /// * `channel_name`: Name of the channel to locate.
    ///
    /// # Returns
    ///
    /// * `Some((u16, u8))`: The channel’s offset and length in the buffer.
    /// * `None`: If the channel is not found.
    pub fn get_channel_location(&self, channel_name: &str) -> Option<(u16, u8)> {
        // Find the accumulator config for the given channel name
        self._accumulators
            .iter()
            .find(|config| config.name == channel_name)
            .map(|config| (config.var_loc, config.var_len))
    }
}
