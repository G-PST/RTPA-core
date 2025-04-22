//! # Synchrophasor Data Processing and PDC Interface
//!
//! This crate provides a comprehensive library for processing IEEE C37.118 synchrophasor
//! data and interfacing with Phasor Data Concentrator (PDC) servers, as defined in
//! IEEE C37.118-2005, IEEE C37.118.2-2011, and IEEE C37.118.2-2024 standards. It supports
//! parsing, generating, and streaming synchrophasor data, accumulating measurements into
//! Arrow record batches, and managing PDC connections for real-time power system monitoring.
//!
//! ## Submodules
//!
//! - `accumulator`: Handles data accumulation for timeseries processing.
//!   - `manager`: Manages accumulators to produce Arrow record batches from data streams.
//!   - `sparse`: Defines accumulators for parsing specific data types from input buffers.
//! - `ieee_c37_118`: Core functionality for IEEE C37.118 frame processing.
//!   - `commands`: Manages command frames for controlling PDC servers (e.g., start/stop streaming).
//!   - `common`: Defines shared types (e.g., `ParseError`, `Version`) for frame handling.
//!   - `config`: Parses configuration frames (CFG-1, CFG-2, CFG-3) for PMU metadata.
//!   - `data_frame`: Processes data frames with real-time synchrophasor measurements.
//!   - `phasors`: Handles phasor measurements in polar or rectangular formats.
//!   - `random`: Generates random frames for testing parsing logic.
//!   - `units`: Defines measurement units (e.g., `PhasorUnits`, `NominalFrequency`).
//!   - `sparse`: Accumulators for IEEE C37.118 data types (e.g., phasors, timestamps).
//! - `utils`: Utility functions for data processing.
//!   - `config_to_accumulators`: Maps configuration frames to accumulator configurations.
//! - `pdc_buffer`: Interfaces with PDC servers over TCP, managing connections and data streams.
//!
//! ## Usage
//!
//! This crate is designed for applications in power system monitoring, such as parsing PMU
//! data, streaming from PDC servers, or testing synchrophasor processing logic. It leverages
//! Arrow for efficient timeseries storage and supports parallel processing for real-time
//! performance, making it suitable for grid monitoring and analysis.

pub mod accumulator {
    pub mod manager;

    pub mod sparse;
}

pub mod ieee_c37_118;
pub mod utils;

pub mod pdc_buffer;
