// The PDCBuffer aims to be a centralized module for interacting with a PDC server.
// It provides a convenient interface for managing the connection to the PDC server,
// parsing the configuration, and managing the data stream.

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
use crate::ieee_c37_118::utils::validate_checksum;
use crate::utils::config_to_accumulators;

const DEFAULT_STANDARD: Version = Version::V2011;
const DEFAULT_MAX_BATCHES: usize = 120; // ~4 minutes at 60hz
const DEFAULT_BATCH_SIZE: usize = 128; // ~2 seconds of data at 60hz

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
    // Methods go here

    pub fn new(
        ip_addr: String,
        port: u16,
        id_code: u16,
        version: Option<Version>,
        batch_size: Option<usize>,
        max_batches: Option<usize>,
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
        // ensure frame size is less than 56 kB
        if frame_size > 56 * 1024 {
            panic!("Frame size exceeds maximum allowable limit of 56KB!")
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

        let accumulators = config_to_accumulators(&config_frame);

        let accumulator_manager = AccumulatorManager::new_with_params(
            accumulators.clone(),
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
                    if validate_checksum(&frame) == false {
                        println!("Invalid Checksum, Skipping buffer.")
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

    pub fn config_to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&self.config_frame)
    }

    pub fn get_pdc_configuration(&self) {
        // Get the configuration
        // This should return the PDC Configuration frame Struct, which
        // has a method to convert to json.
    }

    pub fn get_pdc_header(&self) {
        // Get the header
        // TODO: implement the header frame.
        todo!()
    }

    pub fn set_stream_channels(&mut self, _channels: Vec<String>, _channel_type: String) {
        // Set the stream channels
        // Set the accumulators by picking specific channels, or PMUs or stations.
        // Must be in the lists of channels, PMUs, or stations.
        // Channel type can be "channel", "pmu", or "station".
        // TODO: implement the channel type.
        todo!()
    }

    pub fn list_channels(&self) -> Vec<String> {
        // Lists the set of channels as strings based on the config.
        self._channels.clone()
    }

    pub fn list_pmus(&self) -> Vec<String> {
        // Lists the names of PMUs as strings based on the config.
        self._pmus.clone()
    }

    pub fn list_stations(&self) -> Vec<String> {
        // Lists the names of the stations as strings based on the config.
        self._stations.clone()
    }

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

    pub fn get_channel_location(&self, channel_name: &str) -> Option<(u16, u8)> {
        // Find the accumulator config for the given channel name
        self._accumulators
            .iter()
            .find(|config| config.name == channel_name)
            .map(|config| (config.var_loc, config.var_len))
    }
}
