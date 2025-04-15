// This module provides functions for generating random PDC systems.
// A random PDC system is equivalent to a random configuration frame.
// From a given configuration frame, random or representing a real system, generate random data frames.
//
use crate::ieee_c37_118::frames_v2::{
    ConfigurationFrame1and2_2011, DataFrame2011, PMUConfigurationFrame2011, PMUDataFrame,
    PMUDataFrameFixedFreq2011, PMUDataFrameFloatFreq2011, PMUFrameType, PrefixFrame2011,
};
use crate::ieee_c37_118::utils::calculate_crc;
use crate::ieee_c37_118::{ConfigurationFrame, DataFrame, VersionStandard};
use rand::{thread_rng, Rng};
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_NUM_PMUS: usize = 10; // Should be two stations
const DEFAULT_VERSION: VersionStandard = VersionStandard::Ieee2011;
const DEFAULT_POLAR: bool = true;

// Generate random station name (16 bytes)
fn random_station_name(index: usize) -> [u8; 16] {
    let mut name = [0u8; 16];
    let name_str = format!("STATION{:02}", index);
    let bytes = name_str.as_bytes();
    name[..bytes.len().min(16)].copy_from_slice(&bytes[..bytes.len().min(16)]);
    name
}

// Generate random channel name (16 bytes)
fn random_channel_name(prefix: &str, index: usize) -> [u8; 16] {
    let mut name = [0u8; 16];
    let name_str = format!("{}_{:02}", prefix, index);
    let bytes = name_str.as_bytes();
    name[..bytes.len().min(16)].copy_from_slice(&bytes[..bytes.len().min(16)]);
    name
}

fn random_pmu_chunk() -> Vec<u8> {
    // Generate random data for a PMU in a data frame
    let mut rng = thread_rng();
    let mut data = Vec::new();

    // STAT field (2 bytes)
    data.extend_from_slice(&rng.gen::<u16>().to_be_bytes());

    // For simplicity, we'll just generate some random bytes that would be interpreted
    // based on the configuration frame later
    let chunk_size = rng.gen_range(20..100); // Random size for the data chunk
    for _ in 0..chunk_size {
        data.push(rng.gen());
    }

    data
}

fn create_random_pmu_config(
    station_index: usize,
    is_polar: bool,
    use_float: bool,
) -> PMUConfigurationFrame2011 {
    let mut rng = thread_rng();

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
    let phnmr: u16 = rng.gen_range(1..4); // 1-3 phasors
    let annmr: u16 = rng.gen_range(0..3); // 0-2 analog values
    let dgnmr: u16 = rng.gen_range(0..2); // 0-1 digital status words

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
    for i in 0..dgnmr {
        let name = random_channel_name("DG", i as usize);
        chnam.extend_from_slice(&name);
    }

    // Generate conversion factors
    let phunit: Vec<u32> = (0..phnmr).map(|_| rng.gen()).collect();
    let anunit: Vec<u32> = (0..annmr).map(|_| rng.gen()).collect();
    let digunit: Vec<u32> = (0..dgnmr).map(|_| rng.gen()).collect();

    PMUConfigurationFrame2011 {
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
        fnom: 0x0001, // 60Hz nominal frequency
        cfgcnt: rng.gen(),
    }
}

pub fn random_configuration_frame(
    num_pmus: Option<usize>,
    version: Option<VersionStandard>,
    polar: Option<bool>,
) -> Box<dyn ConfigurationFrame> {
    let mut rng = thread_rng();

    // Set defaults or use provided values
    let num_pmus = num_pmus.unwrap_or(DEFAULT_NUM_PMUS);
    let version = version.unwrap_or(DEFAULT_VERSION);
    let is_polar = polar.unwrap_or(DEFAULT_POLAR);

    // Calculate how many stations we need (1 station per 5 PMUs)
    let num_stations = (num_pmus + 4) / 5;
    let use_float = true; // Always use float for simplicity

    // Create prefix frame
    let frame_type_code = match version {
        VersionStandard::Ieee2005 => 0xA2, // Config1 frame for 2005
        VersionStandard::Ieee2011 => 0xA2, // Config1 frame for 2011
        VersionStandard::Ieee2024 => 0xA3, // Config1 frame for 2024
        VersionStandard::Other(v) => 0xA0 | (v & 0x0F), // Custom version
    };

    let version_code = match version {
        VersionStandard::Ieee2005 => 0x01,
        VersionStandard::Ieee2011 => 0x02,
        VersionStandard::Ieee2024 => 0x03,
        VersionStandard::Other(v) => v,
    };

    let sync: u16 = (frame_type_code as u16) << 8 | version_code as u16;

    // Get current time
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    let soc = now.as_secs() as u32;
    let fracsec = ((now.subsec_nanos() as f64) / 1_000_000_000.0 * 16777216.0) as u32; // 24-bit fraction

    // Create PMU configurations
    let mut pmu_configs = Vec::new();
    for i in 0..num_stations {
        pmu_configs.push(create_random_pmu_config(i, is_polar, use_float));
    }

    // Create configuration frame
    let mut config_frame = ConfigurationFrame1and2_2011 {
        prefix: PrefixFrame2011 {
            sync,
            framesize: 0, // Will be calculated later
            idcode: rng.gen(),
            soc,
            fracsec,
        },
        time_base: 16777216, // Standard time base for C37.118
        num_pmu: num_stations as u16,
        pmu_configs,
        data_rate: 30, // 30 frames per second
        chk: 0,        // Will be calculated later
    };

    // Calculate frame size
    let mut frame_bytes = Vec::new();

    // Prefix (14 bytes)
    frame_bytes.extend_from_slice(&config_frame.prefix.to_hex());

    // TIME_BASE (4 bytes)
    frame_bytes.extend_from_slice(&config_frame.time_base.to_be_bytes());

    // NUM_PMU (2 bytes)
    frame_bytes.extend_from_slice(&config_frame.num_pmu.to_be_bytes());

    // PMU configurations
    for pmu in &config_frame.pmu_configs {
        // STN (16 bytes)
        frame_bytes.extend_from_slice(&pmu.stn);

        // IDCODE (2 bytes)
        frame_bytes.extend_from_slice(&pmu.idcode.to_be_bytes());

        // FORMAT (2 bytes)
        frame_bytes.extend_from_slice(&pmu.format.to_be_bytes());

        // PHNMR (2 bytes)
        frame_bytes.extend_from_slice(&pmu.phnmr.to_be_bytes());

        // ANNMR (2 bytes)
        frame_bytes.extend_from_slice(&pmu.annmr.to_be_bytes());

        // DGNMR (2 bytes)
        frame_bytes.extend_from_slice(&pmu.dgnmr.to_be_bytes());

        // CHNAM
        frame_bytes.extend_from_slice(&pmu.chnam);

        // PHUNIT
        for unit in &pmu.phunit {
            frame_bytes.extend_from_slice(&unit.to_be_bytes());
        }

        // ANUNIT
        for unit in &pmu.anunit {
            frame_bytes.extend_from_slice(&unit.to_be_bytes());
        }

        // DIGUNIT
        for unit in &pmu.digunit {
            frame_bytes.extend_from_slice(&unit.to_be_bytes());
        }

        // FNOM (2 bytes)
        frame_bytes.extend_from_slice(&pmu.fnom.to_be_bytes());

        // CFGCNT (2 bytes)
        frame_bytes.extend_from_slice(&pmu.cfgcnt.to_be_bytes());
    }

    // DATA_RATE (2 bytes)
    frame_bytes.extend_from_slice(&config_frame.data_rate.to_be_bytes());

    // Add 2 bytes for checksum
    config_frame.prefix.framesize = (frame_bytes.len() + 2) as u16;

    // Update framesize in the bytes
    frame_bytes[2..4].copy_from_slice(&config_frame.prefix.framesize.to_be_bytes());

    // Calculate checksum
    let checksum = calculate_crc(&frame_bytes);
    config_frame.chk = checksum;

    Box::new(config_frame)
}

pub fn random_data_frame(configuration_frame: &dyn ConfigurationFrame) -> Box<dyn DataFrame> {
    let mut rng = thread_rng();

    // Downcast the configuration frame to the 2011 version
    let config_frame = match configuration_frame
        .as_any()
        .downcast_ref::<ConfigurationFrame1and2_2011>()
    {
        Some(frame) => frame,
        None => panic!("Configuration frame is not of type ConfigurationFrame1and2_2011"),
    };

    // Create prefix frame with current timestamp
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    let soc = now.as_secs() as u32;
    let fracsec = ((now.subsec_nanos() as f64) / 1_000_000_000.0 * 16777216.0) as u32;

    let prefix = PrefixFrame2011 {
        sync: 0xAA01, // Data frame (0x01) with version 1
        framesize: 0, // Will be calculated later
        idcode: config_frame.prefix.idcode,
        soc,
        fracsec,
    };

    // Generate random PMU data frames based on configuration
    let mut pmu_data = Vec::new();

    for pmu_config in &config_frame.pmu_configs {
        // Create a data frame for each PMU in the configuration
        if pmu_config.format & 0x0008 != 0 {
            // Float frequency
            let data = generate_float_pmu_data(pmu_config);
            pmu_data.push(PMUFrameType::Floating(data));
        } else {
            // Fixed frequency
            let data = generate_fixed_pmu_data(pmu_config);
            pmu_data.push(PMUFrameType::Fixed(data));
        }
    }

    // Create the data frame
    let mut data_frame = DataFrame2011 {
        prefix,
        data: pmu_data,
        chk: 0, // Will be calculated later
    };

    // Calculate frame size by converting to bytes
    let mut frame_bytes = Vec::new();

    // Prefix (14 bytes)
    frame_bytes.extend_from_slice(&data_frame.prefix.to_hex());

    // Add PMU data
    for pmu in &data_frame.data {
        match pmu {
            PMUFrameType::Floating(data) => {
                // STAT
                frame_bytes.extend_from_slice(&data.stat.to_be_bytes());

                // Phasors
                frame_bytes.extend_from_slice(&data.phasors);

                // FREQ (4 bytes for float)
                frame_bytes.extend_from_slice(&data.freq.to_be_bytes());

                // DFREQ (4 bytes for float)
                frame_bytes.extend_from_slice(&data.dfreq.to_be_bytes());

                // Analog data
                frame_bytes.extend_from_slice(&data.analog);

                // Digital data
                frame_bytes.extend_from_slice(&data.digital);
            }
            PMUFrameType::Fixed(data) => {
                // STAT
                frame_bytes.extend_from_slice(&data.stat.to_be_bytes());

                // Phasors
                frame_bytes.extend_from_slice(&data.phasors);

                // FREQ (2 bytes for fixed)
                frame_bytes.extend_from_slice(&data.freq.to_be_bytes());

                // DFREQ (2 bytes for fixed)
                frame_bytes.extend_from_slice(&data.dfreq.to_be_bytes());

                // Analog data
                frame_bytes.extend_from_slice(&data.analog);

                // Digital data
                frame_bytes.extend_from_slice(&data.digital);
            }
        }
    }

    // Add 2 bytes for checksum
    data_frame.prefix.framesize = (frame_bytes.len() + 2) as u16;

    // Update framesize in the bytes
    frame_bytes[2..4].copy_from_slice(&data_frame.prefix.framesize.to_be_bytes());

    // Calculate checksum
    let checksum = calculate_crc(&frame_bytes);
    data_frame.chk = checksum;

    Box::new(data_frame)
}

fn generate_float_pmu_data(pmu_config: &PMUConfigurationFrame2011) -> PMUDataFrameFloatFreq2011 {
    let mut rng = thread_rng();

    // Create random STAT (2 bytes)
    let stat = 0; // Using 0 for normal operation

    // Generate random phasors
    let mut phasors = Vec::new();
    let phasor_size = pmu_config.phasor_size();
    let total_phasor_bytes = phasor_size * pmu_config.phnmr as usize;

    for _ in 0..total_phasor_bytes {
        phasors.push(rng.gen());
    }

    // Generate random frequency and dfreq (floating point)
    let freq = 60.0 + rng.gen::<f32>() * 0.1 - 0.05; // Nominally 60Hz with small variation
    let dfreq = rng.gen::<f32>() * 0.02 - 0.01; // Small variation around zero

    // Generate random analog values
    let mut analog = Vec::new();
    let analog_size = pmu_config.analog_size();
    let total_analog_bytes = analog_size * pmu_config.annmr as usize;

    for _ in 0..total_analog_bytes {
        analog.push(rng.gen());
    }

    // Generate random digital values
    let mut digital = Vec::new();
    let total_digital_bytes = 2 * pmu_config.dgnmr as usize; // 2 bytes per digital word

    for _ in 0..total_digital_bytes {
        digital.push(rng.gen());
    }

    PMUDataFrameFloatFreq2011 {
        stat,
        phasors,
        freq,
        dfreq,
        analog,
        digital,
    }
}

fn generate_fixed_pmu_data(pmu_config: &PMUConfigurationFrame2011) -> PMUDataFrameFixedFreq2011 {
    let mut rng = thread_rng();

    // Create random STAT (2 bytes)
    let stat = 0; // Using 0 for normal operation

    // Generate random phasors
    let mut phasors = Vec::new();
    let phasor_size = pmu_config.phasor_size();
    let total_phasor_bytes = phasor_size * pmu_config.phnmr as usize;

    for _ in 0..total_phasor_bytes {
        phasors.push(rng.gen());
    }

    // Generate random frequency and dfreq (fixed point)
    // For fixed point, frequency is scaled by 1000
    // e.g., 60000 represents 60.000 Hz
    // TODO verify the definition of freq, dfreq
    // We won't really use this setting in the mock-pdc for now.
    let freq = rng.gen_range(1..120) as i16;
    let dfreq = rng.gen_range(1..10) as i16;

    // Generate random analog values
    let mut analog = Vec::new();
    let analog_size = pmu_config.analog_size();
    let total_analog_bytes = analog_size * pmu_config.annmr as usize;

    for _ in 0..total_analog_bytes {
        analog.push(rng.gen());
    }

    // Generate random digital values
    let mut digital = Vec::new();
    let total_digital_bytes = 2 * pmu_config.dgnmr as usize; // 2 bytes per digital word

    for _ in 0..total_digital_bytes {
        digital.push(rng.gen());
    }

    PMUDataFrameFixedFreq2011 {
        stat,
        phasors,
        freq,
        dfreq,
        analog,
        digital,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ieee_c37_118::{parse_configuration_frame, parse_data_frame};

    #[test]
    fn test_create_random_config() {
        // Test different version combinations
        let versions = vec![
            VersionStandard::Ieee2005,
            VersionStandard::Ieee2011,
            // VersionStandard::Ieee2024, // Uncomment if 2024 is supported
        ];

        let polarities = vec![true, false];
        let pmu_counts = vec![2, 5, 20];

        for version in versions {
            for polar in &polarities {
                for num_pmus in &pmu_counts {
                    // Create random configuration frame
                    let config_frame =
                        random_configuration_frame(Some(*num_pmus), Some(version), Some(*polar));

                    // Convert to bytes
                    let bytes = config_frame.to_bytes();

                    // Parse the bytes back to a frame
                    let parsed_frame = parse_configuration_frame(&bytes);

                    // Check if parsing was successful
                    assert!(parsed_frame.is_ok(),
                        "Failed to parse generated config frame: version={:?}, polar={}, num_pmus={}",
                        version, polar, num_pmus);

                    // Further validation could be added here to ensure all fields match
                }
            }
        }
    }

    #[test]
    fn test_create_random_dataframe() {
        // Create a random configuration frame
        let cf1 = random_configuration_frame(None, None, None);

        // Create a random data frame based on the configuration
        let df1 = random_data_frame(cf1.as_ref());

        // Convert to bytes
        let bytes = df1.to_bytes();

        // Parse the bytes back to a frame
        let parsed_frame = parse_data_frame(&bytes, cf1.as_ref());

        // Check if parsing was successful
        assert!(parsed_frame.is_ok(), "Failed to parse generated data frame");

        // Test with specific parameters
        let cf2 = random_configuration_frame(Some(5), Some(VersionStandard::Ieee2011), Some(true));
        let df2 = random_data_frame(cf2.as_ref());
        let bytes2 = df2.to_bytes();
        let parsed_frame2 = parse_data_frame(&bytes2, cf2.as_ref());
        assert!(
            parsed_frame2.is_ok(),
            "Failed to parse data frame with specific parameters"
        );
    }
}
