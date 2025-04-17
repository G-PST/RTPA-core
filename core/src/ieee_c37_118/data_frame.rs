#![allow(unused)]

/// Represents a data frame from IEEE C37.118
use super::common::{ChannelDataType, ParseError, PrefixFrame, StatField};
use super::config::ConfigurationFrame;
use super::utils::{calculate_crc, validate_checksum};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataFrame {
    pub prefix: PrefixFrame,
    pub pmu_data: Vec<PMUData>,
    pub chk: u16,
}

/// Represents data from a single PMU within a data frame
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PMUData {
    pub stat: StatField,
    pub phasors: Vec<u8>, // Raw bytes for phasor values
    pub freq: Vec<u8>,    // Raw bytes for frequency value
    pub dfreq: Vec<u8>,   // Raw bytes for delta frequency value
    pub analog: Vec<u8>,  // Raw bytes for analog values
    pub digital: Vec<u8>, // Raw bytes for digital status words
}

impl DataFrame {
    /// Parse a data frame from hex bytes, using the configuration to determine data formats
    pub fn from_hex(bytes: &[u8], config: &ConfigurationFrame) -> Result<Self, ParseError> {
        // Validate minimum frame size
        if bytes.len() < 16 {
            // PrefixFrame + CHK
            return Err(ParseError::InvalidLength);
        }

        // Validate checksum
        if !validate_checksum(bytes) {
            return Err(ParseError::InvalidChecksum);
        }

        // Parse prefix frame (first 14 bytes)
        let prefix = PrefixFrame::from_hex(&bytes[0..14])?;

        // Check if framesize matches buffer length
        if prefix.framesize as usize != bytes.len() {
            return Err(ParseError::InvalidLength);
        }

        // Parse PMU data sections
        let mut offset = 14; // Start after prefix
        let mut pmu_data = Vec::new();

        for pmu_config in &config.pmu_configs {
            // Check if we have enough bytes left (excluding the checksum)
            if offset + 2 > bytes.len() - 2 {
                return Err(ParseError::InvalidLength);
            }

            // Parse STAT field
            let stat_raw = u16::from_be_bytes([bytes[offset], bytes[offset + 1]]);
            let stat = StatField::from_raw(stat_raw, config.prefix.version);
            offset += 2;

            // Phasor values
            let phasor_size = pmu_config.phasor_size() * pmu_config.phnmr as usize;
            if offset + phasor_size > bytes.len() - 2 {
                return Err(ParseError::InvalidLength);
            }
            let phasors = bytes[offset..offset + phasor_size].to_vec();
            offset += phasor_size;

            // Frequency value
            let freq_size = pmu_config.freq_dfreq_size();
            if offset + freq_size > bytes.len() - 2 {
                return Err(ParseError::InvalidLength);
            }
            let freq = bytes[offset..offset + freq_size].to_vec();
            offset += freq_size;

            // DFREQ value
            if offset + freq_size > bytes.len() - 2 {
                return Err(ParseError::InvalidLength);
            }
            let dfreq = bytes[offset..offset + freq_size].to_vec();
            offset += freq_size;

            // Analog values
            let analog_size = pmu_config.analog_size() * pmu_config.annmr as usize;
            if offset + analog_size > bytes.len() - 2 {
                return Err(ParseError::InvalidLength);
            }
            let analog = bytes[offset..offset + analog_size].to_vec();
            offset += analog_size;

            // Digital status words
            let digital_size = 2 * pmu_config.dgnmr as usize;
            if offset + digital_size > bytes.len() - 2 {
                return Err(ParseError::InvalidLength);
            }
            let digital = bytes[offset..offset + digital_size].to_vec();
            offset += digital_size;

            pmu_data.push(PMUData {
                stat,
                phasors,
                freq,
                dfreq,
                analog,
                digital,
            });
        }

        // Extract checksum from the last two bytes
        let chk = u16::from_be_bytes([bytes[bytes.len() - 2], bytes[bytes.len() - 1]]);

        Ok(DataFrame {
            prefix,
            pmu_data,
            chk,
        })
    }

    /// Convert the data frame to hex bytes
    pub fn to_hex(&self) -> Vec<u8> {
        let mut result = Vec::new();

        // Add prefix frame
        result.extend_from_slice(&self.prefix.to_hex());

        // Add each PMU's data
        for pmu in &self.pmu_data {
            // Add STAT field
            result.extend_from_slice(&pmu.stat.to_raw(self.prefix.version).to_be_bytes());

            // Add phasor data
            result.extend_from_slice(&pmu.phasors);

            // Add frequency data
            result.extend_from_slice(&pmu.freq);

            // Add DFREQ data
            result.extend_from_slice(&pmu.dfreq);

            // Add analog data
            result.extend_from_slice(&pmu.analog);

            // Add digital data
            result.extend_from_slice(&pmu.digital);
        }

        // Add checksum
        let calculated_crc = calculate_crc(&result);
        result.extend_from_slice(&calculated_crc.to_be_bytes());

        result
    }

    /// Retrieve a specific channel value from the data frame
    pub fn get_value(&self, channel_name: &str, config: &ConfigurationFrame) -> Option<DataValue> {
        let channel_map = config.get_channel_map();

        // Find the channel info
        let channel_info = match channel_map.get(channel_name) {
            Some(info) => info,
            None => return None,
        };

        // Extract the raw bytes for this channel from the data frame
        let frame_bytes = self.to_hex();
        if channel_info.offset + channel_info.size > frame_bytes.len() {
            return None;
        }

        let bytes = &frame_bytes[channel_info.offset..channel_info.offset + channel_info.size];

        // Interpret the bytes based on the channel type
        match channel_info.data_type {
            ChannelDataType::FreqFixed => {
                if bytes.len() == 2 {
                    let value = i16::from_be_bytes([bytes[0], bytes[1]]);
                    Some(DataValue::Integer(value as i32))
                } else {
                    None
                }
            }
            ChannelDataType::FreqFloat => {
                if bytes.len() == 4 {
                    let value = f32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                    Some(DataValue::Float(value))
                } else {
                    None
                }
            }
            // Add handlers for other channel types as needed
            _ => None,
        }
    }

    /// Retrieve all channel values from the data frame
    pub fn get_all_values(&self, config: &ConfigurationFrame) -> HashMap<String, DataValue> {
        let mut values = HashMap::new();
        let channel_map = config.get_channel_map();

        for (channel_name, _) in &channel_map {
            if let Some(value) = self.get_value(channel_name, config) {
                values.insert(channel_name.clone(), value);
            }
        }

        values
    }
}

/// Enum to represent different types of channel values
#[derive(Debug, Clone)]
pub enum DataValue {
    Integer(i32),
    Float(f32),
    Complex(f32, f32), // Real, Imaginary
    Polar(f32, f32),   // Magnitude, Angle
    Digital(u16),
}
