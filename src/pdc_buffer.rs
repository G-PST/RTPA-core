// The PDCBuffer aims to be a centralized module for interacting with a PDC server.
// It provides a convenient interface for managing the connection to the PDC server,
// parsing the configuration, and managing the data stream.
//
// A window of data is kept in memory for fast queries.
//
// The module should be structured in a way that it can be imported as a python module.
//

use super::accumulator_manager::AccumulatorManager;
use super::sparse_accumulator::SparseAccumulator;

struct PDCBuffer {
    // Fields and methods go here
    ip_addr: String,
    port: u16,
    _accumulators: Vec<SparseAccumulator>,
    _accumulator_manager: AccumulatorManager,
    _ieee_version: String, // Should make this an enum
    _config_frame: Vec<u8>,
    _batch_size: usize,
    _channels: Vec<String>,
    _pmus: Vec<String>,
    _stations: Vec<String>,
}

impl PDCBuffer {
    // Methods go here
    pub fn new(ip_addr: String, port: u16) -> Self {
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

        PDCBuffer {
            // Initialize fields here
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
        //
    }
    pub fn stop_stream(&mut self) {
        // Try to send the command frame to stop the stream.
        // If unable, throw a warning and close the TCP connection.
        //
        // If successful, close the TCP connection.
        // Stop the threads.
    }

    pub fn get_pdc_configuration(&self) -> String {
        // Get the configuration
        // This should return the PDC Configuration frame Struct, which
        // has a method to convert to json.
    }
    pub fn get_pdc_header(&self) -> String {
        // Get the header
        // TODO: implement the header frame.
    }
    pub fn set_stream_channels(&mut self, channels: Vec<String>, channel_type: String) {
        // Set the stream channels
        // Set the accumulators by picking specific channels, or PMUs or stations.
        // Must be in the lists of channels, PMUs, or stations.
        // Channel type can be "channel", "pmu", or "station".
        // TODO: implement the channel type.
    }
    pub fn list_channels(&self) -> Vec<String> {
        // List the channels
    }
    pub fn list_pmus(&self) -> Vec<String> {
        // List the PMUs
    }
    pub fn list_stations(&self) -> Vec<String> {
        // List the stations
    }
    fn _set_accumulators(&mut self, accumulators: Vec<f64>) {
        // Set the accumulators
    }
    pub fn get_pdc_dataframes(&self) -> Vec<String> {
        // Get the dataframes and returns them as a RecordBatch
    }
    pub fn get_latest_buffer(&self) -> Vec<String> {
        // Get the latest incoming serialized buffer from the PDC server.
    }
}
