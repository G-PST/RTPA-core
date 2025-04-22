//! # IEEE C37.118 Synchrophasor Data Processing
//!
//! This crate provides a comprehensive library for parsing, generating, and testing
//! IEEE C37.118 synchrophasor data frames, used in power system monitoring and control,
//! as defined in IEEE C37.118-2005, IEEE C37.118.2-2011, and IEEE C37.118.2-2024
//! standards. It supports configuration, data, command, and phasor measurement frames,
//! along with utilities for scaling and testing.
//!
//! ## Submodules
//!
//! - `commands`: Handles command frames for controlling synchrophasor devices (e.g.,
//!   starting/stopping data transmission).
//! - `common`: Defines shared types (e.g., `ParseError`, `PrefixFrame`, `Version`) for
//!   frame parsing and construction.
//! - `config`: Manages configuration frames (CFG-1, CFG-2, CFG-3) for describing PMU
//!   configurations and channel metadata.
//! - `data_frame`: Processes data frames containing real-time synchrophasor measurements
//!   from PMUs.
//! - `phasors`: Provides structures for parsing and converting phasor measurements in
//!   polar or rectangular formats.
//! - `random`: Generates random configuration and data frames for testing parsing logic.
//! - `units`: Defines measurement units (e.g., `PhasorUnits`, `NominalFrequency`) for
//!   configuration frames.
//! - `utils`: Offers helper functions (e.g., CRC checksum calculations) for frame
//!   validation.
//!
//! ## Usage
//!
//! This crate is designed for applications processing synchrophasor data, such as PMU
//! data parsers, power system monitoring tools, or test suites. It supports all major
//! frame types and provides utilities for testing and validation, ensuring compliance
//! with IEEE C37.118 standards.

pub mod commands;
pub mod common;
pub mod config;
pub mod data_frame;
pub mod phasors;
pub mod random;
pub mod units;
pub mod utils;

#[cfg(test)]
mod tests;
