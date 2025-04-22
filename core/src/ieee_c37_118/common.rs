//! # IEEE C37.118 Common Types and Utilities
//!
//! This module defines core types and utilities for parsing and constructing IEEE C37.118
//! synchrophasor frames, used in power system monitoring and control. It includes error
//! handling, version tracking, frame type definitions, and prefix frame structures
//! compliant with IEEE C37.118-2005, IEEE C37.118.2-2011, and IEEE C37.118.2-2024
//! standards.
//!
//! ## Key Components
//!
//! - `ParseError`: Enumerates errors encountered during frame parsing, such as invalid
//!   length, checksum, or version.
//! - `Version`: Tracks IEEE C37.118 standard versions (2005, 2011, 2024) based on the
//!   SYNC field.
//! - `FrameType`: Represents frame types (e.g., Data, Header, Configuration) as defined
//!   in the SYNC field.
//! - `PrefixFrame`: Defines the common prefix structure for all IEEE C37.118 frames,
//!   including SYNC, frame size, ID code, and time fields.
//! - `StatField`: Interprets the STAT field, with version-specific flags for data errors,
//!   time quality, and triggers.
//! - `ChannelDataType` and `ChannelInfo`: Support parsing of channel data (e.g., phasors,
//!   frequency) in frames.
//!
//! ## Usage
//!
//! This module is used by other crates in the repository to parse and validate IEEE
//! C37.118 frames, ensuring compatibility across different standard versions. It provides
//! low-level utilities for constructing and interpreting frame headers and status fields.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Represents errors that can occur during IEEE C37.118 frame parsing.
///
/// This enum captures various parsing failures, such as invalid frame length, checksum
/// mismatches, or unsupported versions, as defined in IEEE C37.118 standards.
///
/// # Variants
///
/// * `InvalidLength`: Frame length is too short or incorrect.
/// * `InvalidFrameType`: Frame type in the SYNC field is invalid.
/// * `InvalidChecksum`: CRC checksum does not match the calculated value.
/// * `InvalidFormat`: Frame format does not conform to the standard.
/// * `InvalidHeader`: Header frame content is malformed.
/// * `VersionNotSupported`: Version in the SYNC field is not supported.
/// * `UnknownVersion`: Version in the SYNC field is unrecognized.
/// * `InvalidPhasorType`: Phasor data type is invalid.
#[derive(Debug)]
pub enum ParseError {
    InvalidLength { message: String },
    InvalidFrameType { message: String },
    InvalidChecksum { message: String },
    InvalidFormat { message: String },
    InvalidHeader { message: String },
    VersionNotSupported { message: String },
    UnknownVersion { message: String },
    InvalidPhasorType { message: String },
}
impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ParseError::InvalidLength { message } => write!(f, "Invalid length: {}", message),
            ParseError::InvalidFrameType { message } => {
                write!(f, "Invalid frame type: {}", message)
            }
            ParseError::InvalidChecksum { message } => write!(f, "Invalid checksum: {}", message),
            ParseError::InvalidFormat { message } => write!(f, "Invalid format: {}", message),
            ParseError::InvalidHeader { message } => write!(f, "Invalid header: {}", message),
            ParseError::VersionNotSupported { message } => {
                write!(f, "Version not supported: {}", message)
            }
            ParseError::UnknownVersion { message } => write!(f, "Unknown version: {}", message),
            ParseError::InvalidPhasorType { message } => {
                write!(f, "Invalid phasor type: {}", message)
            }
        }
    }
}

/// Tracks the IEEE C37.118 standard version based on the SYNC field.
///
/// This enum represents the supported versions of the IEEE C37.118 standard, used to
/// interpret frame structures and fields correctly.
///
/// # Variants
///
/// * `V2005`: IEEE C37.118-2005 (SYNC version 0x0001).
/// * `V2011`: IEEE C37.118.2-2011 (SYNC version 0x0002).
/// * `V2024`: IEEE C37.118.2-2024 (SYNC version 0x0003).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Version {
    V2005, // IEEE C37.118-2005 (version 0x0001)
    V2011, // IEEE C37.118.2-2011 (version 0x0010)
    V2024, // IEEE C37.118.2-2024 (version 0x0011)
}

impl Version {
    /// Creates a `Version` from the SYNC field’s version bits.
    ///
    /// # Parameters
    ///
    /// * `sync`: The 16-bit SYNC field containing version bits (3-0).
    ///
    /// # Returns
    ///
    /// * `Ok(Version)`: The corresponding version (V2005, V2011, or V2024).
    /// * `Err(ParseError::UnknownVersion)`: If the version bits are unrecognized.
    pub fn from_sync(sync: u16) -> Result<Self, ParseError> {
        match sync & 0x000F {
            0x0001 => Ok(Version::V2005),
            0x0002 => Ok(Version::V2011),
            0x003 => Ok(Version::V2024),
            _ => Err(ParseError::UnknownVersion {
                message: format!("Unsupported version: 0x{:04X}", sync),
            }),
        }
    }
    /// Creates a `Version` from a string identifier.
    ///
    /// # Parameters
    ///
    /// * `s`: A string representing the standard (e.g., "IEEE Std C37.118-2005", "v1").
    ///
    /// # Returns
    ///
    /// * `Ok(Version)`: The corresponding version.
    /// * `Err(ParseError::UnknownVersion)`: If the string is unrecognized.
    pub fn from_string(s: &str) -> Result<Self, ParseError> {
        match s {
            "IEEE Std C37.118-2005" | "version1" | "v1" => Ok(Version::V2005),
            "IEEE Std C37.118.2-2011" | "version2" | "v2" => Ok(Version::V2011),
            "IEEE Std C37.118.2-2024" | "version3" | "v3" => Ok(Version::V2024),
            _ => Err(ParseError::UnknownVersion {
                message: format!(
                    "Use one of the following:
                    IEEE Std C37.118-2005, IEEE Std C37.118.2-2011, IEEE Std C37.118.2-2024
                    version1, version2, version3
                    v1, v2, v3
                    "
                ),
            }),
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

/// Constructs a SYNC field for an IEEE C37.118 frame.
///
/// Combines the leading byte (0xAA), frame type bits, and version bits as per
/// IEEE C37.118 standards.
///
/// # Parameters
///
/// * `version`: The IEEE C37.118 version (V2005, V2011, or V2024).
/// * `frame_type`: The frame type (e.g., Data, Header, Config1).
///
/// # Returns
///
/// A 16-bit SYNC field value.
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

//TODO Implement TimeQualityIndicator struct

/// Represents the type of an IEEE C37.118 frame.
///
/// This enum defines the possible frame types encoded in the SYNC field’s bits 6-4,
/// as specified in IEEE C37.118 standards.
///
/// # Variants
///
/// * `Data`: Data frame containing synchrophasor measurements.
/// * `Header`: Header frame with descriptive information.
/// * `Config1`: Configuration frame 1 (device capabilities).
/// * `Config2`: Configuration frame 2 (current configuration).
/// * `Config3`: Configuration frame 3 (extended configuration, 2024).
/// * `Command`: Command frame for control instructions.
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
    /// Extracts the frame type from the SYNC field.
    ///
    /// # Parameters
    ///
    /// * `sync`: The 16-bit SYNC field containing frame type bits (6-4).
    ///
    /// # Returns
    ///
    /// * `Ok(FrameType)`: The corresponding frame type.
    /// * `Err(ParseError::InvalidFrameType)`: If the frame type bits or leading byte
    ///   (0xAA) are invalid.
    pub fn from_sync(sync: u16) -> Result<FrameType, ParseError> {
        // Verify first byte is 0xAA
        if (sync >> 8) != 0xAA {
            return Err(ParseError::InvalidFrameType {
                message: format!("Invalid first byte: {}, expected 0xAA", (sync >> 8)),
            });
        }
        let frame_type_bits = (sync >> 4) & 0x7;

        match frame_type_bits {
            0 => Ok(FrameType::Data),
            1 => Ok(FrameType::Header),
            2 => Ok(FrameType::Config1),
            3 => Ok(FrameType::Config2),
            4 => Ok(FrameType::Command),
            5 => Ok(FrameType::Config3),
            _ => Err(ParseError::InvalidFrameType {
                message: format!("Invalid frame type bits: {}", frame_type_bits),
            }),
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

/// Defines the data type for a channel in an IEEE C37.118 frame.
///
/// This enum specifies the format of channel data, such as phasors, frequency, or
/// analog/digital values, used in data and configuration frames.
///
/// # Variants
///
/// * `PhasorIntRectangular`: Integer rectangular phasor (real, imaginary).
/// * `PhasorIntPolar`: Integer polar phasor (magnitude, angle).
/// * `PhasorFloatRectangular`: Floating-point rectangular phasor.
/// * `PhasorFloatPolar`: Floating-point polar phasor.
/// * `FreqFixed`: Fixed-point frequency.
/// * `FreqFloat`: Floating-point frequency.
/// * `DfreqFixed`: Fixed-point frequency deviation.
/// * `DfreqFloat`: Floating-point frequency deviation.
/// * `AnalogFixed`: Fixed-point analog value.
/// * `AnalogFloat`: Floating-point analog value.
/// * `Digital`: Digital status value.
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

/// Stores metadata for a channel in an IEEE C37.118 frame.
///
/// This struct describes a channel’s data type, byte offset, and size, used for
/// parsing data or configuration frames.
#[derive(Debug, Clone)]
pub struct ChannelInfo {
    pub data_type: ChannelDataType,
    pub offset: usize,
    pub size: usize,
}

/// Represents the common prefix structure for IEEE C37.118 frames.
///
/// This struct encapsulates the mandatory fields present in all IEEE C37.118 frame
/// types, including SYNC, frame size, ID code, and timestamp fields, as defined in
/// IEEE C37.118-2005, 2011, and 2024 standards.
///
/// # Fields
///
/// * `sync`: 16-bit SYNC field (frame type and version).
/// * `framesize`: Total frame length in bytes.
/// * `idcode`: Device identification code or stream identifier.
/// * `soc`: Second-of-century timestamp (Unix epoch).
/// * `leapbyte`: Time quality and leap second flags.
/// * `fracsec`: Fractional second timestamp.
/// * `version`: Derived IEEE C37.118 version (not serialized).
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
    /// Creates a new `PrefixFrame` with default values.
    ///
    /// # Parameters
    ///
    /// * `sync`: 16-bit SYNC field.
    /// * `idcode`: Device identification code.
    /// * `version`: IEEE C37.118 version.
    ///
    /// # Returns
    ///
    /// A `PrefixFrame` with default frame size (14 bytes) and zeroed timestamp fields.
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

    /// Parses a `PrefixFrame` from a byte slice.
    ///
    /// # Parameters
    ///
    /// * `bytes`: Byte slice containing at least 14 bytes of frame data.
    ///
    /// # Returns
    ///
    /// * `Ok(PrefixFrame)`: The parsed prefix frame.
    /// * `Err(ParseError::InvalidLength)`: If the byte slice is too short.
    pub fn from_hex(bytes: &[u8]) -> Result<Self, ParseError> {
        if bytes.len() < 14 {
            return Err(ParseError::InvalidLength {
                message: format!(
                    "Too few bytes to parse PrefixFrame: Expected at least 14 bytes, but got {}",
                    bytes.len()
                ),
            });
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

    /// Converts the `PrefixFrame` to a 14-byte array.
    ///
    /// # Returns
    ///
    /// A 14-byte array representing the prefix frame.
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

/// Represents the STAT field in IEEE C37.118 data frames.
///
/// This struct interprets the 16-bit STAT field, which contains status flags for data
/// errors, PMU synchronization, time quality, and triggers, with version-specific
/// meanings (2005, 2011, 2024).
///
/// # Fields
///
/// * `raw`: Raw 16-bit STAT value.
/// * `data_error`: 2-bit data error code.
/// * `pmu_sync`: PMU synchronization status.
/// * `data_sorting`: Data sorting status.
/// * `pmu_trigger`: PMU trigger status.
/// * `config_change`: Configuration change flag.
/// * `data_modified`: Data modified flag (2011/2024 only).
/// * `time_quality`: Time quality code (3-bit in 2011/2024, 2-bit in 2005).
/// * `unlock_time`: Unlock time code (2011/2024 only).
/// * `trigger_reason`: 4-bit trigger reason code.
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
    /// Creates a `StatField` from a raw STAT value and version.
    ///
    /// # Parameters
    ///
    /// * `raw`: 16-bit STAT field value.
    /// * `version`: IEEE C37.118 version.
    ///
    /// # Returns
    ///
    /// A `StatField` with parsed status flags.
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

    /// Converts the `StatField` to a raw 16-bit STAT value.
    ///
    /// # Parameters
    ///
    /// * `version`: IEEE C37.118 version.
    ///
    /// # Returns
    ///
    /// The 16-bit STAT field value.
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
