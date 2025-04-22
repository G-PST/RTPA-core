//! # IEEE C37.118 Test Frame Generator
//!
//! This module provides utilities for generating random IEEE C37.118 configuration and
//! data frames for testing purposes, as defined in IEEE C37.118-2005, IEEE C37.118.2-2011,
//! and IEEE C37.118.2-2024 standards. These frames simulate realistic synchrophasor data
//! structures for validating parsing and processing logic.
//!
//! ## Key Components
//!
//! - `random_configuration_frame`: Generates a random `ConfigurationFrame` with specified
//!   PMUs, version, and phasor format.
//! - `random_data_frame`: Generates a random `DataFrame` based on a given configuration.
//! - `create_random_pmu_config`: Internal utility for creating random PMU configurations.
//! - `random_pmu_data`: Internal utility for generating random PMU data.
//! - `random_station_name`, `random_channel_name`: Helpers for generating names.
//!
//! ## Usage
//!
//! This module is used in testing to create synthetic configuration and data frames,
//! ensuring compatibility with IEEE C37.118 standards. It integrates with the `common`,
//! `config`, `data_frame`, `units`, and `utils` modules for frame structures and utilities.

use super::common::{create_sync, FrameType, PrefixFrame, StatField, Version};
use super::config::{ConfigurationFrame, PMUConfigurationFrame};
use super::data_frame::{DataFrame, PMUData};
use super::units::{AnalogUnits, MeasurementType, NominalFrequency, PhasorUnits};
use super::utils::{calculate_crc, now_to_hex};
use rand::Rng;
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_NUM_PMUS: usize = 10;
const DEFAULT_VERSION: Version = Version::V2005;
const DEFAULT_POLAR: bool = false;

/// Generates a random 16-byte station name for a PMU.
///
/// # Parameters
///
/// * `index`: Station index for unique naming.
///
/// # Returns
///
/// A 16-byte array containing a station name (e.g., "STATION01").
fn random_station_name(index: usize) -> [u8; 16] {
    let mut name = [b' '; 16];
    let name_str = format!("STATION{:02}", index);
    let bytes = name_str.as_bytes();
    name[..bytes.len().min(16)].copy_from_slice(&bytes[..bytes.len().min(16)]);
    name
}

/// Generates a random 16-byte channel name for a measurement.
///
/// # Parameters
///
/// * `prefix`: Channel type prefix (e.g., "PH" for phasor, "AN" for analog).
/// * `index`: Channel index for unique naming.
///
/// # Returns
///
/// A 16-byte array containing a channel name (e.g., "PH_01").
fn random_channel_name(prefix: &str, index: usize) -> [u8; 16] {
    let mut name = [b' '; 16];
    let name_str = format!("{}_{:02}", prefix, index);
    let bytes = name_str.as_bytes();
    name[..bytes.len().min(16)].copy_from_slice(&bytes[..bytes.len().min(16)]);
    name
}

/// Creates a random PMU configuration for testing.
///
/// # Parameters
///
/// * `station_index`: Index for station naming and ID code.
/// * `is_polar`: Whether phasors are in polar format (`true`) or rectangular (`false`).
/// * `use_float`: Whether to use floating-point formats for phasors, analogs, and frequency.
///
/// # Returns
///
/// A `PMUConfigurationFrame` with random but valid configuration data.
fn create_random_pmu_config(
    station_index: usize,
    is_polar: bool,
    use_float: bool,
) -> PMUConfigurationFrame {
    // Generate format field based on parameters
    let mut format: u16 = 0;
    if is_polar {
        format |= 0x0001; // Bit 0: 1 for polar
    }
    if use_float {
        format |= 0x0002; // Bit 1: 1 for float phasors
        format |= 0x0004; // Bit 2: 1 for float analogs
        format |= 0x0008; // Bit 3: 1 for float freq/dfreq
    }

    // Determine number of each type of measurement
    let phnmr: u16 = 4; //rng.random_range(1..4); // 1-3 phasors
    let annmr: u16 = 3; //rng.random_range(0..3); // 0-2 analog values
    let dgnmr: u16 = 1; //rng.random_range(0..2); // 0-1 digital status words

    // Generate channel names
    let mut chnam = Vec::new();

    // Phasor names
    for i in 0..phnmr {
        let name = random_channel_name("PH", i as usize);
        chnam.extend_from_slice(&name);
    }

    // Analog names
    for i in 0..annmr {
        let name = random_channel_name("AN", i as usize);
        chnam.extend_from_slice(&name);
    }

    // Digital names
    for i in 0..dgnmr * 16 {
        let name = random_channel_name("DG", i as usize);
        chnam.extend_from_slice(&name);
    }
    println!(
        "RANDOM:Created Channel Names of total length {}",
        chnam.len()
    );

    // Generate conversion factors
    let phunit: Vec<PhasorUnits> = vec![
        PhasorUnits {
            is_current: false,
            _scale_factor: 915527,
        },
        PhasorUnits {
            is_current: false,
            _scale_factor: 915527,
        },
        PhasorUnits {
            is_current: false,
            _scale_factor: 915527,
        },
        PhasorUnits {
            is_current: true,
            _scale_factor: 45776,
        },
    ];
    let anunit: Vec<AnalogUnits> = vec![
        AnalogUnits {
            measurement_type: MeasurementType::SinglePointOnWave,
            scale_factor: 1,
        },
        AnalogUnits {
            measurement_type: MeasurementType::RmsOfAnalogInput,
            scale_factor: 1,
        },
        AnalogUnits {
            measurement_type: MeasurementType::PeakOfAnalogInput,
            scale_factor: 1,
        },
    ];
    let digunit: Vec<u32> = vec![0];

    PMUConfigurationFrame {
        stn: random_station_name(station_index),
        idcode: (1000 + station_index) as u16,
        format,
        phnmr,
        annmr,
        dgnmr,
        chnam,
        phunit,
        anunit,
        digunit,
        fnom: NominalFrequency::Hz50, // 50Hz nominal frequency
        cfgcnt: 0,
    }
}

/// Generates a random configuration frame for testing IEEE C37.118 compliance.
///
/// Creates a `ConfigurationFrame` with random PMU configurations, simulating a realistic
/// setup for testing parsing and processing logic, as defined in IEEE C37.118 standards.
///
/// # Parameters
///
/// * `num_pmus`: Optional number of PMUs (defaults to 10).
/// * `version`: Optional IEEE C37.118 version (defaults to 2005).
/// * `polar`: Optional flag for polar phasor format (defaults to rectangular).
///
/// # Returns
///
/// A `ConfigurationFrame` with random PMU configurations, valid frame size, and checksum.
pub fn random_configuration_frame(
    num_pmus: Option<usize>,
    version: Option<Version>,
    polar: Option<bool>,
) -> ConfigurationFrame {
    // Set defaults or use provided values
    let num_stations = num_pmus.unwrap_or(DEFAULT_NUM_PMUS);
    let version = version.unwrap_or(DEFAULT_VERSION);
    let is_polar = polar.unwrap_or(DEFAULT_POLAR);

    let use_float = true; // Always use float for simplicity

    // Define frame type based on version

    let sync = create_sync(version, FrameType::Config1);
    // Get current time
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    let soc = now.as_secs() as u32;
    let fracsec = ((now.subsec_nanos() as f64) / 1_000_000_000.0 * 16777216.0) as u32; // 24-bit fraction

    // Create PMU configurations
    let mut pmu_configs = Vec::new();
    for i in 0..num_stations {
        println!("Creating PMU config for station {}", i);
        let cfg = create_random_pmu_config(i, is_polar, use_float);
        let cfg_bytes = cfg.to_hex();

        println!(
            "Added PMU configuration bytes of length: {}",
            cfg_bytes.len()
        );
        pmu_configs.push(cfg);
    }

    // Create initial configuration frame with temporary framesize
    let prefix = PrefixFrame {
        sync,
        framesize: 0, // Will be calculated later
        idcode: 123,
        soc,
        leapbyte: 0,
        fracsec,
        version,
    };

    // Create the configuration frame
    let mut config_frame = ConfigurationFrame {
        prefix,
        time_base: 1_000_000, // Standard time base for C37.118
        num_pmu: num_stations as u16,
        pmu_configs,
        data_rate: 30, // 30 frames per second
        chk: 0,        // Will be calculated later
        cfg_type: 1,
    };

    // Now set the framesize by having ConfigurationFrame calculate it
    // (but don't include the checksum yet)
    let frame_bytes = config_frame.to_hex();

    config_frame.prefix.framesize = frame_bytes.len() as u16;
    println!(
        "RANDOM: Calculated Framesize: {}",
        config_frame.prefix.framesize
    );
    println!("RANDOM: Actual Framesize: {}", frame_bytes.len());

    // Calculate checksum
    config_frame.chk = calculate_crc(&frame_bytes[..frame_bytes.len() - 2]);

    config_frame
}

/// Generates random PMU data based on a PMU configuration for testing.
///
/// # Parameters
///
/// * `pmu_config`: The PMU configuration defining data formats and sizes.
///
/// # Returns
///
/// A `PMUData` instance with random but valid measurement data.
fn random_pmu_data(pmu_config: &PMUConfigurationFrame) -> PMUData {
    let mut rng = rand::rng();

    // Create random STAT field (normally would be 0 for good data)
    let raw_stat: u16 = 0; // Using 0 for normal operation
    let stat = StatField {
        raw: raw_stat,
        data_error: 0,        // No data error
        pmu_sync: true,       // PMU is synchronized
        data_sorting: false,  // No sorting
        pmu_trigger: false,   // No trigger
        config_change: false, // No config change
        data_modified: false, // Data not modified
        time_quality: 0,      // Good time quality
        unlock_time: 0,       // Time is locked (using 0 instead of boolean)
        trigger_reason: 0,    // No trigger reason
    };

    // Generate phasor data
    let mut phasors = Vec::new();
    let phasor_size = pmu_config.phasor_size() * pmu_config.phnmr as usize;
    for _ in 0..phasor_size {
        phasors.push(4);
    }

    // Generate frequency and dfreq data
    let mut freq = Vec::new();
    let mut dfreq = Vec::new();
    //let freq_size = pmu_config.freq_dfreq_size();

    if pmu_config.format & 0x0008 != 0 {
        // Float frequency (4 bytes)
        let freq_val: f32 = 60.0 + rng.random::<f32>() * 0.2 - 0.1; // Around 60 Hz
        let dfreq_val: f32 = rng.random::<f32>() * 0.1 - 0.05; // Small df/dt

        freq.extend_from_slice(&freq_val.to_be_bytes());
        dfreq.extend_from_slice(&dfreq_val.to_be_bytes());
    } else {
        // Fixed frequency (2 bytes) - scaled value
        let freq_val: i16 = rng.random::<i16>(); // Random frequency
        let dfreq_val: i16 = rng.random::<i16>(); // Small df/dt

        freq.extend_from_slice(&freq_val.to_be_bytes());
        dfreq.extend_from_slice(&dfreq_val.to_be_bytes());
    }

    // Generate analog values
    let mut analog = Vec::new();
    let analog_size = pmu_config.analog_size() * pmu_config.annmr as usize;
    for _ in 0..analog_size {
        analog.push(rng.random::<u8>());
    }

    // Generate digital values
    let mut digital = Vec::new();
    let total_digital_bytes = 2 * pmu_config.dgnmr as usize;
    for _ in 0..total_digital_bytes {
        digital.push(rng.random::<u8>());
    }

    PMUData {
        stat,
        phasors,
        freq,
        dfreq,
        analog,
        digital,
    }
}

/// Generates a random data frame for testing IEEE C37.118 compliance.
///
/// Creates a `DataFrame` with random measurement data based on a provided configuration
/// frame, simulating real-time synchrophasor data, as defined in IEEE C37.118 standards.
///
/// # Parameters
///
/// * `config_frame`: The configuration frame defining PMU data formats and sizes.
///
/// # Returns
///
/// A `DataFrame` with random PMU data, valid frame size, and checksum.
pub fn random_data_frame(config_frame: &ConfigurationFrame) -> DataFrame {
    // Create prefix frame with current timestamp

    let soc_fracsecs = now_to_hex(config_frame.time_base);
    // Extract the SOC from the first 4 bytes
    let soc = u32::from_be_bytes([
        soc_fracsecs[0],
        soc_fracsecs[1],
        soc_fracsecs[2],
        soc_fracsecs[3],
    ]);

    // Extract the fracsec from the last 4 bytes
    let fracsec = u32::from_be_bytes([
        soc_fracsecs[4],
        soc_fracsecs[5],
        soc_fracsecs[6],
        soc_fracsecs[7],
    ]);

    // Data frame has a different sync word
    let sync = create_sync(config_frame.prefix.version, FrameType::Data);

    let prefix = PrefixFrame {
        sync,
        framesize: 0, // Will be calculated later
        idcode: config_frame.prefix.idcode,
        soc,
        leapbyte: 0,
        fracsec,
        version: config_frame.prefix.version,
    };

    // Generate random PMU data based on configuration
    let mut pmu_data = Vec::new();

    for pmu_config in &config_frame.pmu_configs {
        pmu_data.push(random_pmu_data(pmu_config));
    }

    // Create the data frame
    let mut data_frame = DataFrame {
        prefix,
        pmu_data,
        chk: 0, // Will be calculated later
    };

    // Calculate the framesize using to_hex()
    let frame_bytes = data_frame.to_hex();
    data_frame.prefix.framesize = frame_bytes.len() as u16;

    // Generate the frame bytes again with the correct framesize
    let frame_bytes = data_frame.to_hex();

    // Calculate checksum
    data_frame.chk = calculate_crc(&frame_bytes[..frame_bytes.len() - 2]);

    data_frame
}

#[cfg(test)]
mod tests {
    use super::*;

    //#[ignore]
    #[test]
    fn test_random_config_frame() {
        // Test with default parameters
        let config_frame = random_configuration_frame(Some(1), None, None);

        // Verify the frame has the expected number of PMUs
        assert_eq!(config_frame.num_pmu, 1 as u16);

        // Verify the frame can be converted to bytes
        let bytes = config_frame.to_hex();
        assert!(!bytes.is_empty());

        // Verify the actual size matches the expected size
        assert_eq!(bytes.len(), config_frame.prefix.framesize as usize);

        // Test with custom parameters
        let custom_config = random_configuration_frame(Some(10), Some(Version::V2011), Some(true));

        let _custom_config_parsed = ConfigurationFrame::from_hex(&custom_config.to_hex()).unwrap();
        // Verify the frame has the expected number of PMUs
        assert_eq!(custom_config.num_pmu, 10);

        // Verify the frame can be converted to bytes
        let custom_bytes = custom_config.to_hex();
        assert!(!custom_bytes.is_empty());

        // Verify the actual size matches the expected size
        assert_eq!(custom_bytes.len(), custom_config.prefix.framesize as usize);
    }

    #[test]
    fn test_random_data_frame() {
        // Create a random configuration frame
        let config_frame = random_configuration_frame(Some(5), None, None);

        // Create a random data frame based on the configuration
        let data_frame = random_data_frame(&config_frame);

        // Verify the data frame has the same number of PMUs as the config frame
        assert_eq!(data_frame.pmu_data.len(), config_frame.pmu_configs.len());

        // Verify the data frame can be converted to bytes
        let bytes = data_frame.to_hex();
        assert!(!bytes.is_empty());

        // Verify the actual size matches the expected size
        assert_eq!(bytes.len(), data_frame.prefix.framesize as usize);
    }
}
