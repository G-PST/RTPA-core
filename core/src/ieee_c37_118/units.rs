#![allow(unused)]
use super::common::ParseError;
use serde::{Deserialize, Serialize};
use std::fmt;

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
    pub fn from_hex(bytes: &[u8]) -> Result<Self, ParseError> {
        if bytes.len() != 4 {
            return Err(ParseError::InvalidLength);
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
    // TODO decide whether to convert all to f32 or keep scale factors in int.
    pub fn scale_factor(&self) -> u32 {
        (self._scale_factor / i16::MAX as u32) * 10e5 as u32
    }
}

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
    fn from_hex(byte: &u8) -> Result<Self, ParseError> {
        match byte {
            0 => Ok(MeasurementType::SinglePointOnWave),
            1 => Ok(MeasurementType::RmsOfAnalogInput),
            2 => Ok(MeasurementType::PeakOfAnalogInput),
            _ => Err(ParseError::InvalidFormat),
        }
    }
    fn to_hex(&self) -> Result<u8, ParseError> {
        match self {
            MeasurementType::SinglePointOnWave => Ok(0),
            MeasurementType::RmsOfAnalogInput => Ok(1),
            MeasurementType::PeakOfAnalogInput => Ok(2),
            _ => Err(ParseError::InvalidFormat),
        }
    }
}
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
    pub fn from_hex(bytes: &[u8]) -> Result<Self, ParseError> {
        let measurement_type = MeasurementType::from_hex(&bytes[0])?;
        let scale_factor = i32::from_be_bytes([0, bytes[1], bytes[2], bytes[3]]);
        Ok(AnalogUnits {
            measurement_type,
            scale_factor,
        })
    }
    pub fn to_hex(&self) -> [u8; 4] {
        let mut bytes = [0u8; 4];
        bytes[0] = self.measurement_type.to_hex().unwrap();
        //let scale_factor_bytes = &self.scale_factor.to_be_bytes()[1..];
        bytes[1..].copy_from_slice(&self.scale_factor.to_be_bytes()[1..]);
        bytes
    }
}
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NominalFrequency {
    Hz50,
    Hz60,
}
impl NominalFrequency {
    pub fn from_hex(byte: &[u8]) -> Result<Self, ParseError> {
        match byte[0] {
            0 => Ok(NominalFrequency::Hz50),
            1 => Ok(NominalFrequency::Hz60),
            _ => Err(ParseError::InvalidFormat),
        }
    }
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataRate {
    // Rate of phasor data transmission in Hz (Frames per second).
    // If data rate > 0, rate number is frames per second.
    // if data rate < 0, rate number is seconds per frame.
    pub _value: i16,
}
impl DataRate {
    pub fn from_hex(bytes: &[u8; 2]) -> Result<Self, ParseError> {
        let value = i16::from_be_bytes([bytes[0], bytes[1]]);
        Ok(DataRate { _value: value })
    }
    pub fn to_hex(&self) -> [u8; 2] {
        self._value.to_be_bytes()
    }
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
