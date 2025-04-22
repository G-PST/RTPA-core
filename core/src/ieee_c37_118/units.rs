//! # IEEE C37.118 Measurement Units and Configuration Utilities
//!
//! This module provides structures for defining measurement units and configuration
//! parameters for IEEE C37.118 synchrophasor data, used in power system monitoring, as
//! defined in IEEE C37.118-2005, IEEE C37.118.2-2011, and IEEE C37.118.2-2024 standards.
//! These utilities support parsing and serializing phasor, analog, digital, frequency,
//! and data rate configurations.
//!
//! ## Key Components
//!
//! - `PhasorUnits`: Defines scaling factors for phasor measurements (voltage or current).
//! - `AnalogUnits`: Specifies measurement types and scaling for analog channels.
//! - `DigitalUnits`: Placeholder for digital status word masks (incomplete).
//! - `NominalFrequency`: Indicates the nominal system frequency (50 Hz or 60 Hz).
//! - `DataRate`: Defines the rate of phasor data transmission (frames per second).
//!
//! ## Usage
//!
//! This module is used in configuration frames to specify how measurement data (e.g.,
//! phasors, analogs) should be scaled and interpreted. It integrates with the `common`
//! module for error handling and is used by configuration and data frame parsing logic.

use super::common::ParseError;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Defines scaling factors for phasor measurements in IEEE C37.118.
///
/// This struct specifies whether a phasor represents voltage or current and provides a
/// scaling factor for integer phasor data, as defined in IEEE C37.118 standards (e.g.,
/// Table 9 in IEEE C37.118-2011).
///
/// # Fields
///
/// * `is_current`: Indicates if the phasor is current (`true`) or voltage (`false`).
/// * `_scale_factor`: 24-bit unsigned scaling factor (padded to 32 bits) in 10⁻⁵ V or A
///   per bit, ignored for floating-point phasors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhasorUnits {
    // Most significant bit indicates voltage or current, 0=>Voltage, 1=>Current
    pub is_current: bool,

    // Least significant bytes: Scale factor for the phasor units. A u24 value integer we will pad to u32.
    // "An unsigned 24-bit word in 10-5 V or Amperes per bit to scal 16-bit integer data.
    // If transmitted data is in floating-point format this 24-bit value is ignored."
    pub _scale_factor: u32,
}
impl PhasorUnits {
    /// Parses phasor units from a 4-byte slice.
    ///
    /// # Parameters
    ///
    /// * `bytes`: Byte slice containing 4 bytes (1 for type, 3 for scale factor).
    ///
    /// # Returns
    ///
    /// * `Ok(PhasorUnits)`: The parsed phasor units.
    /// * `Err(ParseError::InvalidLength)`: If the byte slice is not 4 bytes.
    pub fn from_hex(bytes: &[u8]) -> Result<Self, ParseError> {
        if bytes.len() != 4 {
            return Err(ParseError::InvalidLength {
                message: format!(
                    "Invalid length for PhasorUnits: expected 4 bytes, got {}",
                    bytes.len()
                ),
            });
        }
        let mut is_current = false;
        if bytes[0] == 1 {
            is_current = true;
        }

        let scale_factor = u32::from_be_bytes([0, bytes[1], bytes[2], bytes[3]]);
        Ok(PhasorUnits {
            is_current,
            _scale_factor: scale_factor,
        })
    }
    /// Converts the phasor units to a 4-byte array.
    ///
    /// # Returns
    ///
    /// A 4-byte array containing the type and scale factor in big-endian format.
    pub fn to_hex(&self) -> [u8; 4] {
        let mut bytes = [0u8; 4];
        if self.is_current {
            bytes[0] = 1;
        }
        let scale_bytes = self._scale_factor.to_be_bytes();
        bytes[1] = scale_bytes[1];
        bytes[2] = scale_bytes[2];
        bytes[3] = scale_bytes[3];

        bytes
    }

    /// Calculates the effective scaling factor for phasor measurements.
    ///
    /// # Returns
    ///
    /// The scaling factor adjusted for 16-bit integer data (in 10⁵ V or A per bit).
    ///
    /// # Note
    ///
    /// This method is incomplete and may need revision to match IEEE C37.118 scaling rules.
    pub fn scale_factor(&self) -> u32 {
        (self._scale_factor / i16::MAX as u32) * 10e5 as u32
    }
}

/// Enumerates measurement types for analog channels in IEEE C37.118.
///
/// This enum defines the types of analog measurements, as specified in IEEE C37.118
/// standards, used in configuration frames to describe analog channel data.
///
/// # Variants
///
/// * `SinglePointOnWave`: Instantaneous analog value.
/// * `RmsOfAnalogInput`: Root mean square of the analog input.
/// * `PeakOfAnalogInput`: Peak value of the analog input.
/// * `Reserved`: Reserved or unknown measurement type (with raw code).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MeasurementType {
    SinglePointOnWave,
    RmsOfAnalogInput,
    PeakOfAnalogInput,
    Reserved(u8),
}
impl fmt::Display for MeasurementType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MeasurementType::SinglePointOnWave => write!(f, "Single Point-On-Wave"),
            MeasurementType::RmsOfAnalogInput => write!(f, "RMS"),
            MeasurementType::PeakOfAnalogInput => write!(f, "Peak"),
            MeasurementType::Reserved(code) => write!(f, "Unknown ({})", code),
        }
    }
}
impl MeasurementType {
    /// Parses a measurement type from a single byte.
    ///
    /// # Parameters
    ///
    /// * `byte`: The byte encoding the measurement type.
    ///
    /// # Returns
    ///
    /// * `Ok(MeasurementType)`: The parsed measurement type.
    /// * `Err(ParseError::InvalidFormat)`: If the byte is invalid.
    fn from_hex(byte: &u8) -> Result<Self, ParseError> {
        match byte {
            0 => Ok(MeasurementType::SinglePointOnWave),
            1 => Ok(MeasurementType::RmsOfAnalogInput),
            2 => Ok(MeasurementType::PeakOfAnalogInput),
            _ => Err(ParseError::InvalidFormat {
                message: format!(
                    "Invalid format for MeasurementType: expected 0-{}, 1-{}, or 2-{}, got {}",
                    MeasurementType::SinglePointOnWave,
                    MeasurementType::RmsOfAnalogInput,
                    MeasurementType::PeakOfAnalogInput,
                    byte
                ),
            }),
        }
    }
    /// Converts the measurement type to a single byte.
    ///
    /// # Returns
    ///
    /// * `Ok(u8)`: The byte encoding the measurement type.
    /// * `Err(ParseError::InvalidFormat)`: If the type is `Reserved`.
    fn to_hex(&self) -> Result<u8, ParseError> {
        match self {
            MeasurementType::SinglePointOnWave => Ok(0),
            MeasurementType::RmsOfAnalogInput => Ok(1),
            MeasurementType::PeakOfAnalogInput => Ok(2),
            _ => Err(ParseError::InvalidFormat {
                message: format!("Unable to convert MeasurementType-{} to hex", self),
            }),
        }
    }
}

/// Defines scaling and type for analog channels in IEEE C37.118.
///
/// This struct specifies the measurement type and scaling factor for analog channels,
/// as defined in IEEE C37.118 standards, used in configuration frames.
///
/// # Fields
///
/// * `measurement_type`: Type of analog measurement (e.g., RMS, peak).
/// * `scale_factor`: 24-bit signed scaling factor (padded to 32 bits), user-defined.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnalogUnits {
    // Conversion factor for analog channels. Four bytes for each analog value.
    // Most significant byte: 0=>single point-on-wave, 1=> rms of analog input, 2=> peak of analog input, 5-64=>reserved.
    // Least significant bytes: A signed 24-bit integer, user defined scaling.
    //
    pub measurement_type: MeasurementType,
    pub scale_factor: i32, //padded i24
}
impl AnalogUnits {
    /// Parses analog units from a 4-byte slice.
    ///
    /// # Parameters
    ///
    /// * `bytes`: Byte slice containing 4 bytes (1 for type, 3 for scale factor).
    ///
    /// # Returns
    ///
    /// * `Ok(AnalogUnits)`: The parsed analog units.
    /// * `Err(ParseError)`: If the byte slice is invalid or too short.
    pub fn from_hex(bytes: &[u8]) -> Result<Self, ParseError> {
        let measurement_type = MeasurementType::from_hex(&bytes[0])?;
        let scale_factor = i32::from_be_bytes([0, bytes[1], bytes[2], bytes[3]]);
        Ok(AnalogUnits {
            measurement_type,
            scale_factor,
        })
    }
    /// Converts the analog units to a 4-byte array.
    ///
    /// # Returns
    ///
    /// A 4-byte array containing the measurement type and scale factor in big-endian format.
    pub fn to_hex(&self) -> [u8; 4] {
        let mut bytes = [0u8; 4];
        bytes[0] = self.measurement_type.to_hex().unwrap();
        //let scale_factor_bytes = &self.scale_factor.to_be_bytes()[1..];
        bytes[1..].copy_from_slice(&self.scale_factor.to_be_bytes()[1..]);
        bytes
    }
}

/// Placeholder for digital status word masks in IEEE C37.118.
///
/// This struct is intended to define mask words for digital status channels, as specified
/// in IEEE C37.118 standards, but is currently incomplete.
///
/// # Fields
///
/// * `mask_words`: Two 16-bit words (normal status and valid inputs masks).
///
/// # Note
///
/// This is a placeholder with a TODO for full implementation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DigitalUnits {
    // Mask words for digital status words. Two 16-bit words are provided for each digital word.
    // The first will be used to indicate the normal status of the digital inputs by returning 0 when exclusive
    // ORed (XOR) with the status word.
    //
    // The second will indicate the current valid inputs to the PMU by having a bit set in the binary position corresponding to the digital input
    // and all other bits set to 0.
    //

    // TODO
    mask_words: [u16; 2],
}

/// Specifies the nominal system frequency in IEEE C37.118.
///
/// This enum defines the nominal frequency of the power system, as specified in
/// IEEE C37.118 standards, used in configuration frames.
///
/// # Variants
///
/// * `Hz50`: 50 Hz nominal frequency.
/// * `Hz60`: 60 Hz nominal frequency.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NominalFrequency {
    Hz50,
    Hz60,
}
impl NominalFrequency {
    /// Parses the nominal frequency from a byte slice.
    ///
    /// # Parameters
    ///
    /// * `byte`: Byte slice containing at least 1 byte (0 for 50 Hz, 1 for 60 Hz).
    ///
    /// # Returns
    ///
    /// * `Ok(NominalFrequency)`: The parsed frequency.
    /// * `Err(ParseError::InvalidFormat)`: If the byte is invalid.
    pub fn from_hex(byte: &[u8]) -> Result<Self, ParseError> {
        match byte[0] {
            0 => Ok(NominalFrequency::Hz50),
            1 => Ok(NominalFrequency::Hz60),
            _ => Err(ParseError::InvalidFormat {
                message: format!(
                    "Invalid NominalFrequency: Expected 0-{} or 1-{}, got {}",
                    NominalFrequency::Hz50,
                    NominalFrequency::Hz60,
                    byte[0]
                ),
            }),
        }
    }
    /// Converts the nominal frequency to a 2-byte array.
    ///
    /// # Returns
    ///
    /// A 2-byte array encoding the frequency (0 for 50 Hz, 1 for 60 Hz).
    pub fn to_hex(&self) -> Result<[u8; 2], ParseError> {
        match self {
            NominalFrequency::Hz50 => Ok([0u8; 2]),
            NominalFrequency::Hz60 => Ok([1, 0]),
        }
    }
}
impl fmt::Display for NominalFrequency {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            NominalFrequency::Hz50 => write!(f, "50 Hz"),
            NominalFrequency::Hz60 => write!(f, "60 Hz"),
        }
    }
}

/// Defines the phasor data transmission rate in IEEE C37.118.
///
/// This struct specifies the rate of data frame transmission, as defined in IEEE C37.118
/// standards, used in configuration frames to indicate frames per second or seconds per
/// frame.
///
/// # Fields
///
/// * `_value`: 16-bit signed integer (positive for frames per second, negative for
///   seconds per frame).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataRate {
    // Rate of phasor data transmission in Hz (Frames per second).
    // If data rate > 0, rate number is frames per second.
    // if data rate < 0, rate number is seconds per frame.
    pub _value: i16,
}
impl DataRate {
    /// Parses the data rate from a 2-byte slice.
    ///
    /// # Parameters
    ///
    /// * `bytes`: 2-byte slice containing the data rate.
    ///
    /// # Returns
    ///
    /// * `Ok(DataRate)`: The parsed data rate.
    /// * `Err(ParseError)`: If the byte slice is invalid.
    pub fn from_hex(bytes: &[u8; 2]) -> Result<Self, ParseError> {
        let value = i16::from_be_bytes([bytes[0], bytes[1]]);
        Ok(DataRate { _value: value })
    }

    /// Converts the data rate to a 2-byte array.
    ///
    /// # Returns
    ///
    /// A 2-byte array containing the data rate in big-endian format.
    pub fn to_hex(&self) -> [u8; 2] {
        self._value.to_be_bytes()
    }

    /// Calculates the data rate in frames per second.
    ///
    /// # Returns
    ///
    /// The data rate as a float (frames per second for positive values, 1/seconds per
    /// frame for negative values).
    pub fn frequency(&self) -> f32 {
        if self._value > 0 {
            self._value as f32
        } else {
            1.0 / (-self._value as f32)
        }
    }
}

#[cfg(test)]
mod phasor_config_tests {
    use super::*;

    #[test]
    pub fn test_phasor_units() {
        let phunit1: [u8; 4] = [0x00, 0x0D, 0xF8, 0x47];
        let phunit2: [u8; 4] = [0x01, 0x00, 0xB2, 0xD0];

        let p1 = PhasorUnits::from_hex(&phunit1).unwrap();
        let p2 = PhasorUnits::from_hex(&phunit2).unwrap();

        assert_eq!(p1.is_current, false);
        assert_eq!(p2.is_current, true);

        assert_eq!(p1._scale_factor, 915527);
        assert_eq!(p2._scale_factor, 45776);
    }
    #[test]
    pub fn test_nominal_frequency() {
        let nomfreq1: [u8; 2] = [0x00, 0x00];
        let nomfreq2: [u8; 2] = [0x01, 0x00];

        let n1 = NominalFrequency::from_hex(&nomfreq1).unwrap();
        let n2 = NominalFrequency::from_hex(&nomfreq2).unwrap();

        assert_eq!(n1.to_string(), "50 Hz");
        assert_eq!(n2.to_string(), "60 Hz");

        assert_eq!(n1.to_hex().unwrap(), nomfreq1);
        assert_eq!(n2.to_hex().unwrap(), nomfreq2);
    }
}
