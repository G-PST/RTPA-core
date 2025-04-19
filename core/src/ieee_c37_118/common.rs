#![allow(unused)]
use serde::{Deserialize, Serialize};
use std::fmt;

/// Represents errors that can occur during parsing
#[derive(Debug)]
pub enum ParseError {
    InvalidLength,
    InvalidFrameType,
    InvalidChecksum,
    InvalidFormat,
    InvalidHeader,
    VersionNotSupported, // Add more specific error types as needed
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ParseError::InvalidLength => write!(f, "Invalid length"),
            ParseError::InvalidFrameType => write!(f, "Invalid frame type"),
            ParseError::InvalidChecksum => write!(f, "Invalid checksum"),
            ParseError::InvalidFormat => write!(f, "Invalid format"),
            ParseError::InvalidHeader => write!(f, "Invalid header"),
            ParseError::VersionNotSupported => write!(f, "Version not supported"),
        }
    }
}
// Enum to track standard version based on SYNC field
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Version {
    V2005, // IEEE C37.118-2005 (version 0x0001)
    V2011, // IEEE C37.118.2-2011 (version 0x0010)
    V2024, // IEEE C37.118.2-2024 (version 0x0011)
}

impl Version {
    pub fn from_sync(sync: u16) -> Result<Self, ParseError> {
        match sync & 0x000F {
            0x0001 => Ok(Version::V2005),
            0x0002 => Ok(Version::V2011),
            0x003 => Ok(Version::V2024),
            _ => Err(ParseError::VersionNotSupported),
        }
    }
}
impl Default for Version {
    fn default() -> Self {
        Version::V2011
    }
}
impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Version::V2005 => write!(f, "IEEE Std C37.118-2005"),
            Version::V2011 => write!(f, "IEEE Std C37.118.2-2011"),
            Version::V2024 => write!(f, "IEEE Std C37.118.2-2024"),
        }
    }
}

pub fn create_sync(version: Version, frame_type: FrameType) -> u16 {
    // Frame sync word
    // Leading byte is leading bytes 0xAA
    let leading_byte = 0xAA;

    //Second bytes is frame type and version, divided as follows.
    //Bit 7: reserved.
    // Bit 6-4: 000: Data Frame
    //          001: Header Frame
    //          010: Configuration Frame 1
    //          011: Configuration Frame 2
    //          101: Configuration Frame 3
    //          100: Command Frame
    // Bit 3-0: Version number in binary (1-15)
    // Version 1 -> 0001 Version::2005
    // Version 2 -> 0010 Version::2011
    // Version 3 -> 0011 Version::2024

    let frame_type_bits = match frame_type {
        FrameType::Data => 0,
        FrameType::Header => 1,
        FrameType::Config1 => 2,
        FrameType::Config2 => 3,
        FrameType::Config3 => 5,
        FrameType::Command => 4,
    };

    let version_bits = match version {
        Version::V2005 => 0x01, // 0001 binary
        Version::V2011 => 0x02, // 0010 binary - actually means 2011
        Version::V2024 => 0x03, // 0011 binary - actually means 2024
    };

    // Combine all parts
    ((leading_byte as u16) << 8) | ((frame_type_bits as u16) << 4) | version_bits
}

// TODO Implement TimeQualityIndicator struct

/// Represents the type of the frame
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameType {
    Data,
    Header,
    Config1,
    Config2,
    Config3,
    Command,
}
impl FrameType {
    pub fn from_sync(sync: u16) -> Result<FrameType, ParseError> {
        // Verify first byte is 0xAA
        if (sync >> 8) != 0xAA {
            return Err(ParseError::InvalidFrameType);
        }
        let frame_type_bits = (sync >> 4) & 0x7;

        match frame_type_bits {
            0 => Ok(FrameType::Data),
            1 => Ok(FrameType::Header),
            2 => Ok(FrameType::Config1),
            3 => Ok(FrameType::Config2),
            4 => Ok(FrameType::Command),
            5 => Ok(FrameType::Config3),
            _ => Err(ParseError::InvalidFrameType),
        }
    }
}
impl fmt::Display for FrameType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameType::Data => write!(f, "IEEE Std C37.118 Data Frame"),
            FrameType::Header => write!(f, "IEEE Std C37.118 Header Frame"),
            FrameType::Config1 => write!(f, "IEEE Std C37.118 Configuration Frame 1"),
            FrameType::Config2 => write!(f, "IEEE Std C37.118 Configuration Frame 2"),
            FrameType::Config3 => write!(f, "IEEE Std C37.118 Configuration Frame 3"),
            FrameType::Command => write!(f, "IEEE Std C37.118 Command Frame"),
        }
    }
}

// Placeholder for ChannelDataType and ChannelInfo (unchanged from your code)
#[derive(Debug, Clone, PartialEq)]
pub enum ChannelDataType {
    PhasorIntRectangular,
    PhasorIntPolar,
    PhasorFloatRectangular,
    PhasorFloatPolar,
    FreqFixed,
    FreqFloat,
    DfreqFixed,
    DfreqFloat,
    AnalogFixed,
    AnalogFloat,
    Digital,
}

#[derive(Debug, Clone)]
pub struct ChannelInfo {
    pub data_type: ChannelDataType,
    pub offset: usize,
    pub size: usize,
}

// Unified PrefixFrame for all versions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrefixFrame {
    pub sync: u16, // SYNC field: frame type (bits 6-4), version (bits 3-0)
    pub framesize: u16,
    pub idcode: u16,
    pub soc: u32,
    pub leapbyte: u8, // Includes time quality in bits 31-24
    pub fracsec: u32,
    #[serde(skip)] // Transient field, not serialized
    pub version: Version, // Derived from sync
}

impl PrefixFrame {
    pub fn new(sync: u16, idcode: u16, version: Version) -> Self {
        PrefixFrame {
            sync,
            framesize: 14, // Default, updated later
            idcode,
            soc: 0,
            leapbyte: 0,
            fracsec: 0,
            version,
        }
    }

    pub fn from_hex(bytes: &[u8]) -> Result<Self, ParseError> {
        if bytes.len() < 14 {
            return Err(ParseError::InvalidLength);
        }
        let sync = u16::from_be_bytes([bytes[0], bytes[1]]);
        let version = Version::from_sync(sync).unwrap();

        Ok(PrefixFrame {
            sync,
            framesize: u16::from_be_bytes([bytes[2], bytes[3]]),
            idcode: u16::from_be_bytes([bytes[4], bytes[5]]),
            soc: u32::from_be_bytes([bytes[6], bytes[7], bytes[8], bytes[9]]),
            leapbyte: bytes[10],
            fracsec: u32::from_be_bytes([0, bytes[11], bytes[12], bytes[13]]),
            version,
        })
    }

    pub fn to_hex(&self) -> [u8; 14] {
        let mut result = [0u8; 14];
        result[0..2].copy_from_slice(&self.sync.to_be_bytes());
        result[2..4].copy_from_slice(&self.framesize.to_be_bytes());
        result[4..6].copy_from_slice(&self.idcode.to_be_bytes());
        result[6..10].copy_from_slice(&self.soc.to_be_bytes());
        result[10] = self.leapbyte;

        let fracsec = &self.fracsec.to_be_bytes();
        result[11] = fracsec[1];
        result[12] = fracsec[2];
        result[13] = fracsec[3];
        result
    }
}

// STAT field interpretation, version-specific
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatField {
    pub raw: u16,            // Raw STAT value
    pub data_error: u8,      // 2-bit field (all versions)
    pub pmu_sync: bool,      // Bit 13
    pub data_sorting: bool,  // Bit 12
    pub pmu_trigger: bool,   // Bit 11
    pub config_change: bool, // Bit 10
    pub data_modified: bool, // Bit 9 (2011/2024 only)
    pub time_quality: u8,    // 3-bit field (2011/2024), 2-bit in 2005
    pub unlock_time: u8,     // 2-bit field (2011/2024 only)
    pub trigger_reason: u8,  // 4-bit field (all versions)
}

impl StatField {
    pub fn from_raw(raw: u16, version: Version) -> Self {
        let data_error = ((raw >> 14) & 0x03) as u8;
        let pmu_sync = (raw & 0x2000) != 0; // Bit 13
        let data_sorting = (raw & 0x1000) != 0; // Bit 12
        let pmu_trigger = (raw & 0x0800) != 0; // Bit 11
        let config_change = (raw & 0x0400) != 0; // Bit 10
        let trigger_reason = (raw & 0x000F) as u8; // Bits 3-0

        match version {
            Version::V2005 => StatField {
                raw,
                data_error,
                pmu_sync,
                data_sorting,
                pmu_trigger,
                config_change,
                data_modified: false,                    // Not used in 2005
                time_quality: ((raw >> 8) & 0x03) as u8, // Bits 9-8
                unlock_time: 0,                          // Not used in 2005
                trigger_reason,
            },
            Version::V2011 | Version::V2024 => StatField {
                raw,
                data_error,
                pmu_sync,
                data_sorting,
                pmu_trigger,
                config_change,
                data_modified: (raw & 0x0200) != 0,      // Bit 9
                time_quality: ((raw >> 5) & 0x07) as u8, // Bits 7-5
                unlock_time: ((raw >> 4) & 0x03) as u8,  // Bits 5-4
                trigger_reason,
            },
        }
    }

    pub fn to_raw(&self, version: Version) -> u16 {
        let mut raw = 0;

        // Set data_error (bits 15-14)
        raw |= (self.data_error as u16 & 0x03) << 14;

        // Set individual flag bits
        raw |= (self.pmu_sync as u16) << 13;
        raw |= (self.data_sorting as u16) << 12;
        raw |= (self.pmu_trigger as u16) << 11;
        raw |= (self.config_change as u16) << 10;

        // Set trigger reason (bits 3-0)
        raw |= self.trigger_reason as u16 & 0x000F;

        match version {
            Version::V2005 => {
                // In 2005, time_quality uses bits 9-8
                raw |= ((self.time_quality & 0x03) as u16) << 8;
            }
            Version::V2011 | Version::V2024 => {
                // In 2011/2024, data_modified uses bit 9
                raw |= (self.data_modified as u16) << 9;

                // In 2011/2024, time_quality uses bits 7-5
                raw |= ((self.time_quality & 0x07) as u16) << 5;

                // In 2011/2024, unlock_time uses bits 5-4
                raw |= ((self.unlock_time & 0x03) as u16) << 4;
            }
        }
        raw
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_sync() {
        // Test for V2005 Config1
        let version = Version::V2005;
        let frame_type = FrameType::Config1;
        let sync = create_sync(version, frame_type);
        let sync_bytes = sync.to_be_bytes();
        let expected_bytes: [u8; 2] = [0xAA, 0x21];
        assert_eq!(sync_bytes, expected_bytes, "Failed for V2005 Config1");

        // Test all versions and frame types
        let versions = [Version::V2005, Version::V2011, Version::V2024];
        let frame_types = [
            FrameType::Data,
            FrameType::Header,
            FrameType::Config1,
            FrameType::Config2,
            FrameType::Config3,
            FrameType::Command,
        ];

        for &version in &versions {
            for &frame_type in &frame_types {
                let sync = create_sync(version, frame_type);

                // Validate first byte is 0xAA
                assert_eq!(
                    sync >> 8,
                    0xAA,
                    "First byte not 0xAA for {:?} {:?}",
                    version,
                    frame_type
                );

                // Validate frame type bits
                let frame_type_value = match frame_type {
                    FrameType::Data => 0,
                    FrameType::Header => 1,
                    FrameType::Config1 => 2,
                    FrameType::Config2 => 3,
                    FrameType::Config3 => 5,
                    FrameType::Command => 4,
                };
                assert_eq!(
                    (sync >> 4) & 0x7,
                    frame_type_value,
                    "Frame type bits incorrect for {:?} {:?}",
                    version,
                    frame_type
                );

                // Validate version bits
                let version_value = match version {
                    Version::V2005 => 0x01,
                    Version::V2011 => 0x02,
                    Version::V2024 => 0x03,
                };
                assert_eq!(
                    sync & 0x0F,
                    version_value,
                    "Version bits incorrect for {:?} {:?}",
                    version,
                    frame_type
                );

                // Also test round-trip conversion
                let extracted_frame_type = FrameType::from_sync(sync).unwrap();
                assert_eq!(
                    extracted_frame_type, frame_type,
                    "Round-trip frame type mismatch for {:?} {:?}",
                    version, frame_type
                );
            }
        }
    }
}
