// The PDCBuffer aims to be a centralized module for interacting with a PDC server.
// It provides a convenient interface for managing the connection to the PDC server,
// parsing the configuration, and managing the data stream.
//
// A window of data is kept in memory for fast queries.
//
// The module should be structured in a way that it can be imported as a python module.
//
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::mpsc::{self, Receiver, Sender, SyncSender};
use std::thread::{self, JoinHandle};

use arrow::record_batch::RecordBatch;

use super::accumulator::manager::{AccumulatorConfig, AccumulatorManager};
use super::ieee_c37_118::frames::ConfigurationFrame;
use super::ieee_c37_118::utils::{parse_frame_type, parse_protocol_version};
use super::ieee_c37_118::{serialize_command, Command, VersionStandard};

use crate::ieee_c37_118::{parse_configuration_frame, FrameType};
use crate::utils::config_to_accumulators;

const DEFAULT_STANDARD: VersionStandard = VersionStandard::Ieee2011;

pub struct PDCBuffer {
    ip_addr: String,
    port: u16,
    id_code: u16,
    _accumulators: Vec<AccumulatorConfig>,
    _accumulator_manager: AccumulatorManager, // Thread safe manager.
    _ieee_version: VersionStandard,           // The ieee standard version 1&2 or 3
    _config_frame: Box<dyn ConfigurationFrame>, // The configuration frame of the PDC
    _batch_size: usize,
    _channels: Vec<String>,
    _pmus: Vec<String>,
    _stations: Vec<String>,
    producer_handle: Option<JoinHandle<()>>,
    consumer_handle: Option<JoinHandle<()>>,
    shutdown_tx: Option<Sender<()>>,
}

impl PDCBuffer {
    // Methods go here

    pub fn new(ip_addr: String, port: u16, id_code: u16, version: Option<VersionStandard>) -> Self {
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
        let send_config_cmd: Vec<u8> = serialize_command(
            Command::SendConfig2,
            version.unwrap_or(DEFAULT_STANDARD),
            id_code,
        );

        stream.write_all(&send_config_cmd).unwrap();

        // TODO we need to look at the first few bytes to determin the total size and version.
        // We also need to see if this is a large enough size for the maximum configuration
        // frame size.
        let mut peek_buffer: [u8; 4] = [0u8; 4];

        // Read the SYNC and Frame Size of the configuration frame.
        stream.read_exact(&mut peek_buffer).unwrap();

        let sync = u16::from_be_bytes([peek_buffer[0], peek_buffer[1]]);

        let frame_type: FrameType = parse_frame_type(sync).unwrap();
        println!("Detected Frame: {}", frame_type);

        let detected_version: VersionStandard = parse_protocol_version(sync).unwrap();
        println!("Version: {}", detected_version);

        let frame_size = u16::from_be_bytes([peek_buffer[2], peek_buffer[3]]);
        println!("Frame size: {}", frame_size);
        // ensure frame size is less than 56 kB
        if frame_size > 56 * 1024 {
            panic!("Frame size exceeds maximum allowable limit of 56KB!")
        }

        let remaining_size = frame_size as usize - 4;
        println!("Remaining size {}", remaining_size);
        let mut remaining_buffer = vec![0u8; remaining_size];
        println!("created buffer of length {}", remaining_buffer.len());
        // read the entire buffer up to frame_size
        stream.read_exact(&mut remaining_buffer).unwrap();

        println!("filled buffer with {} bytes", remaining_buffer.len());
        // Combine the peek and remaining buffers
        let mut buffer = Vec::with_capacity(frame_size as usize);
        buffer.extend_from_slice(&peek_buffer); // Add the first 4 bytes
        buffer.extend_from_slice(&remaining_buffer); // Add the rest

        // Parse the bytes into the ConfigurationFrame struct, depending on the version.
        let config_frame: Box<dyn ConfigurationFrame> = parse_configuration_frame(&buffer).unwrap();

        // Once parsed, get the ChannelInfo and convert to a complete vec of available AccumulatorConfigs.
        // TODO, this needs to be updated to use new_with_params()

        let accumulators = config_to_accumulators(&*config_frame);
        let accumulator_manager = AccumulatorManager::new_with_params(
            accumulators.clone(),
            2,
            60 * 2, // TODO add parameter to adjust the window
            // Should look at config frequency and input parameter to initialize window.
            // Would be nice to have an option to extend the window while it is still running.
            55 * 1024, // TODO determine max buffer size based on ConfigurationFrame.
            // Alternatively, could look at largest AccumulatorConfig var_loc + size to determine endpoint.
            128,
        );
        println!("RTPA Buffer Initialization Successful");
        PDCBuffer {
            ip_addr,
            port,
            id_code,
            _accumulators: accumulators,
            _accumulator_manager: accumulator_manager,
            _ieee_version: version.unwrap_or(DEFAULT_STANDARD),
            _config_frame: config_frame,
            _batch_size: 120,
            _channels: vec![],
            _pmus: vec![],
            _stations: vec![],
            producer_handle: None,
            consumer_handle: None,
            shutdown_tx: None,
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

        let consumer_manager = self._accumulator_manager.duplicate();
        let start_stream_cmd =
            serialize_command(Command::StartStream, self._ieee_version, self.id_code);
        let producer = {
            let mut stream = stream.try_clone().unwrap();
            thread::spawn(move || {
                let mut buffer = vec![0; 55 * 1024];

                // TODO we should have the version detected be updated in a field in the struct.

                if stream.write_all(&start_stream_cmd).is_err() {
                    println!("Failed to send START_STREAM command");
                    return;
                }

                loop {
                    if shutdown_rx.try_recv().is_ok() {
                        println!("Producer shutting down");
                        break;
                    }

                    match stream.read_exact(&mut buffer) {
                        Ok(()) => {
                            if tx.send(buffer.clone()).is_err() {
                                println!("Consumer disconnected");
                                break;
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
                    if frame.len() >= 56 * 1024 {
                        println!("Invalid frame size: {}", frame.len());
                        continue;
                    }

                    if let Err(e) = consumer_manager.process_buffer(|buf| {
                        buf[..frame.len()].copy_from_slice(&frame);
                        frame.len()
                    }) {
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
        if let Ok(mut stream) = TcpStream::connect(format!("{}:{}", self.ip_addr, self.port)) {
            let stop_stream_cmd =
                serialize_command(Command::StopStream, self._ieee_version, self.id_code);
            let _ = stream.write_all(&stop_stream_cmd);
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
    pub fn set_stream_channels(&mut self, channels: Vec<String>, channel_type: String) {
        // Set the stream channels
        // Set the accumulators by picking specific channels, or PMUs or stations.
        // Must be in the lists of channels, PMUs, or stations.
        // Channel type can be "channel", "pmu", or "station".
        // TODO: implement the channel type.
    }
    pub fn list_channels(&self) -> Vec<String> {
        self._channels.clone()
    }
    pub fn list_pmus(&self) -> Vec<String> {
        self._pmus.clone()
    }
    pub fn list_stations(&self) -> Vec<String> {
        self._stations.clone()
    }

    fn _set_accumulators(&mut self, accumulators: Vec<f64>) {
        todo!()
        // Set the accumulators
    }
    pub fn get_pdc_dataframes(&self) -> Result<RecordBatch, String> {
        // This is a placeholder - implement the actual logic to get data from accumulator_manager
        //self._accumulator_manager
        todo!()
        //    .get_dataframe()
        //    .map_err(|e| format!("Failed to get dataframes: {:?}", e))
    }
    pub fn get_latest_buffer(&self) -> Vec<String> {
        todo!()
        // Get the latest incoming serialized buffer from the PDC server.
    }
}
