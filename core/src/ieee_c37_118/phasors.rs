//! # IEEE C37.118 Phasor Measurement Utilities
//!
//! This module provides structures and utilities for handling IEEE C37.118 phasor
//! measurements, which represent voltage and current as complex numbers in power system
//! monitoring, as defined in IEEE C37.118-2005, IEEE C37.118.2-2011, and
//! IEEE C37.118.2-2024 standards. Phasors can be in polar or rectangular formats,
//! with integer or floating-point representations, and require scaling per the standard.
//!
//! ## Key Components
//!
//! - `PhasorType`: Enumerates phasor data formats (e.g., `FloatPolar`, `IntRect`).
//! - `PhasorValue`: Enum wrapping specific phasor types (polar or rectangular, integer
//!   or float).
//! - `PhasorFloatPolar`, `PhasorFloatRect`, `PhasorIntPolar`, `PhasorIntRect`:
//!   Structures for specific phasor formats.
//! - `scale_phasor_value`: Scales raw phasor values using PHUNIT factors.
//! - `calc_magnitude`: Calculates the magnitude of a rectangular phasor.
//!
//! ## Usage
//!
//! This module is used to parse, convert, and scale phasor measurements from IEEE C37.118
//! data frames, ensuring accurate representation of voltage and current. It integrates
//! with the `common` module for error handling and is used by data frame parsing logic.

use super::common::ParseError;
use std::fmt;

// The max value that all scale factors are divided by for PHUNIT. SEE IEEE C37.118-2011 TABLE 9
// 10e-5 V per unit divided by max of i16 32,
const SCALE_DENOMINATOR_INVERSE: f32 = 0.00001;

/// Scales a raw phasor value using a PHUNIT conversion factor.
///
/// # Parameters
///
/// * `value`: The raw phasor value (e.g., magnitude or component).
/// * `factor`: The PHUNIT conversion factor (e.g., 915527 for voltage).
///
/// # Returns
///
/// The scaled value in physical units (e.g., volts or amperes).
fn scale_phasor_value(value: f32, factor: u32) -> f32 {
    // IEEE example
    //
    // Conversion Factor description from standard
    // PhasorMAG = PHUNIT X * 0.00001
    // Voltage PHUNIT = (300_000/32768) * 10e5
    // Current PHUNIT = (15_000/32768) * 10e5
    //
    //
    // From config Example 1
    //
    //
    // factor value from bytes = 915527  = 0x00 0x0D 0xF8 0x47 (first byte indicates voltage or current)
    //
    // => ( 300_000/32768 ) * 10e5
    //
    // From Config Example 2
    // factor value from bytes = 45776  = 0x01 0x00 0xB2 0xD0 => 0x00 0x00 0xB2 0xD0 (we set the first byte to 0 always)
    // => ( 15_000/32768 ) * 10e5

    // From Data Message Example (Voltages use scale factor in example 1, Current use scale factor in example 2)
    // 16 bit integer rectangular format. First two bytes are real part, second two bytes are imaginary part.
    // Raw Value - Voltage A: 0x39 0x2B 0x00 0x00
    // Raw Value - Voltage B: 0xE3 0X6A 0xCE 0x7C
    // Raw Value - Voltage C: 0xE3 0x6A 0x00 0x00
    // Raw Value - I1:  0x04 0x44 0x00 0x00
    // Final Values:
    // Voltage A = 14635, Angle = 0.0 => Voltage A = 134.0 kV Angle = 0.0
    // Voltage B = 14635, Angle = 180.0 => Voltage B = 134.0 kV Angle = 180.0
    // Voltage C = 14635, Angle = 0.0 => Voltage C = 134.0 kV Angle = 0.0
    // I1 = 1092, Angle = 0.0 => I1 = 500 A , angle = 0.0
    value * SCALE_DENOMINATOR_INVERSE * factor as f32
}

/// Calculates the magnitude of a rectangular phasor.
///
/// # Parameters
///
/// * `real`: Real component of the phasor.
/// * `imag`: Imaginary component of the phasor.
///
/// # Returns
///
/// The phasor’s magnitude (sqrt(real² + imag²)).
fn calc_magnitude(real: f32, imag: f32) -> f32 {
    (real * real + imag * imag).sqrt()
}

/// Enumerates phasor data formats in IEEE C37.118.
///
/// This enum defines the possible formats for phasor measurements, as specified in
/// IEEE C37.118 standards, including polar or rectangular coordinates and integer or
/// floating-point representations.
///
/// # Variants
///
/// * `FloatPolar`: Floating-point polar format (magnitude, angle).
/// * `FloatRect`: Floating-point rectangular format (real, imaginary).
/// * `IntPolar`: Integer polar format (magnitude, angle).
/// * `IntRect`: Integer rectangular format (real, imaginary).
#[derive(Debug, Clone, Copy)]
pub enum PhasorType {
    FloatPolar,
    FloatRect,
    IntRect,
    IntPolar,
}
impl fmt::Display for PhasorType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PhasorType::FloatPolar => write!(f, "FloatPolar"),
            PhasorType::FloatRect => write!(f, "FloatRect"),
            PhasorType::IntRect => write!(f, "IntRect"),
            PhasorType::IntPolar => write!(f, "IntPolar"),
        }
    }
}
impl Default for PhasorType {
    fn default() -> Self {
        PhasorType::FloatPolar
    }
}
impl PhasorType {
    /// Creates a `PhasorType` from a string identifier.
    ///
    /// # Parameters
    ///
    /// * `s`: String representing the phasor type (e.g., "FloatPolar").
    ///
    /// # Returns
    ///
    /// * `Ok(PhasorType)`: The corresponding phasor type.
    /// * `Err(ParseError::InvalidPhasorType)`: If the string is invalid.
    pub fn from_str(s: &str) -> Result<Self, ParseError> {
        match s {
            "FloatPolar" => Ok(PhasorType::FloatPolar),
            "FloatRect" => Ok(PhasorType::FloatRect),
            "IntRect" => Ok(PhasorType::IntRect),
            "IntPolar" => Ok(PhasorType::IntPolar),
            _ => Err(ParseError::InvalidPhasorType {
                message: format!("Invalid phasor type: {}", s),
            }),
        }
    }
}

/// Represents a phasor measurement value in IEEE C37.118.
///
/// This enum wraps specific phasor types (polar or rectangular, integer or float) to
/// support parsing and conversion of phasor measurements, as defined in IEEE C37.118
/// standards.
///
/// # Variants
///
/// * `FloatPolar`: Floating-point polar phasor.
/// * `FloatRect`: Floating-point rectangular phasor.
/// * `IntPolar`: Integer polar phasor.
/// * `IntRect`: Integer rectangular phasor.
#[derive(Debug, Clone, Copy)]
pub enum PhasorValue {
    FloatPolar(PhasorFloatPolar),
    FloatRect(PhasorFloatRect),
    IntPolar(PhasorIntPolar),
    IntRect(PhasorIntRect),
}

impl PhasorValue {
    /// Parses a phasor value from a byte slice.
    ///
    /// # Parameters
    ///
    /// * `bytes`: Byte slice containing phasor data (4 or 8 bytes).
    /// * `phasor_type`: The phasor’s format (e.g., `FloatPolar`).
    ///
    /// # Returns
    ///
    /// * `Ok(PhasorValue)`: The parsed phasor value.
    /// * `Err(ParseError)`: If the byte slice is too short or invalid.
    pub fn from_hex(
        bytes: &[u8],
        phasor_type: PhasorType,
    ) -> Result<Self, super::common::ParseError> {
        match phasor_type {
            PhasorType::FloatPolar => {
                let phasor = PhasorFloatPolar::from_hex(bytes)?;
                Ok(PhasorValue::FloatPolar(phasor))
            }
            PhasorType::FloatRect => {
                let phasor = PhasorFloatRect::from_hex(bytes)?;
                Ok(PhasorValue::FloatRect(phasor))
            }
            PhasorType::IntPolar => {
                let phasor = PhasorIntPolar::from_hex(bytes)?;
                Ok(PhasorValue::IntPolar(phasor))
            }
            PhasorType::IntRect => {
                let phasor = PhasorIntRect::from_hex(bytes)?;
                Ok(PhasorValue::IntRect(phasor))
            }
        }
    }

    /// Retrieves the phasor’s format.
    ///
    /// # Returns
    ///
    /// The `PhasorType` of the phasor value.
    pub fn get_type(&self) -> PhasorType {
        match self {
            PhasorValue::FloatPolar(_) => PhasorType::FloatPolar,
            PhasorValue::FloatRect(_) => PhasorType::FloatRect,

            PhasorValue::IntRect(_) => PhasorType::IntRect,
            PhasorValue::IntPolar(_) => PhasorType::IntPolar,
        }
    }
    /// Converts the phasor to floating-point rectangular format.
    ///
    /// # Parameters
    ///
    /// * `scale_factor`: Optional PHUNIT scaling factor (required for integer phasors).
    ///
    /// # Returns
    ///
    /// A `PhasorFloatRect` with scaled real and imaginary components.
    ///
    /// # Panics
    ///
    /// Panics if `scale_factor` is `None` for integer phasors.
    pub fn to_float_rect(&self, scale_factor: Option<u32>) -> PhasorFloatRect {
        // Convert a float polar to a float rect
        // or convert a intpolar to float rect
        // or convert a intrect to float rect
        match self {
            PhasorValue::FloatRect(phasor) => *phasor,
            PhasorValue::FloatPolar(phasor) => phasor.to_float_rect(),
            PhasorValue::IntRect(phasor) => {
                let scale = scale_factor.expect("Scale factor is required for IntRect conversion");
                phasor.to_float_rect(scale)
            }
            PhasorValue::IntPolar(phasor) => {
                let scale = scale_factor.expect("Scale factor is required for IntPolar conversion");
                phasor.to_float_rect(scale)
            }
        }
    }
    /// Converts the phasor to floating-point polar format.
    ///
    /// # Parameters
    ///
    /// * `scale_factor`: Optional PHUNIT scaling factor (required for integer phasors).
    ///
    /// # Returns
    ///
    /// A `PhasorFloatPolar` with scaled magnitude and angle.
    ///
    /// # Panics
    ///
    /// Panics if `scale_factor` is `None` for integer phasors.
    pub fn to_float_polar(&self, scale_factor: Option<u32>) -> PhasorFloatPolar {
        // Convert a float rect to a float polar
        // or convert a intrect to float polar (use scale factors)
        // or convert a intpolar to float polar
        // If already float polar return self.
        match self {
            PhasorValue::FloatRect(phasor) => phasor.to_float_polar(),
            PhasorValue::FloatPolar(phasor) => *phasor,
            PhasorValue::IntRect(phasor) => {
                let scale = scale_factor.expect("Scale factor is required for IntRect conversion");
                phasor.to_float_polar(scale)
            }
            PhasorValue::IntPolar(phasor) => {
                let scale = scale_factor.expect("Scale factor is required for IntPolar conversion");
                phasor.to_float_polar(scale)
            }
        }
    }
}

/// Represents a floating-point polar phasor in IEEE C37.118.
///
/// This struct stores a phasor measurement in polar format (magnitude and angle) using
/// 32-bit floating-point values, as defined in IEEE C37.118 standards.
///
/// # Fields
///
/// * `angle`: Phase angle in radians.
/// * `magnitude`: Magnitude in physical units (e.g., volts or amperes).
#[derive(Debug, Clone, Copy)]
pub struct PhasorFloatPolar {
    pub angle: f32,
    pub magnitude: f32,
}
impl PhasorFloatPolar {
    /// Parses a floating-point polar phasor from a byte slice.
    ///
    /// # Parameters
    ///
    /// * `bytes`: Byte slice containing 8 bytes (4 for magnitude, 4 for angle).
    ///
    /// # Returns
    ///
    /// * `Ok(PhasorFloatPolar)`: The parsed phasor.
    /// * `Err(ParseError::InvalidLength)`: If the byte slice is too short.
    pub fn from_hex(bytes: &[u8]) -> Result<Self, super::common::ParseError> {
        if bytes.len() < 8 {
            return Err(super::common::ParseError::InvalidLength {
                message: format!(
                    "Invalid length for PhasorFloatPolar: expected 8 bytes, got {}",
                    bytes.len()
                ),
            });
        }

        let magnitude = f32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let angle = f32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);

        Ok(PhasorFloatPolar { magnitude, angle })
    }

    /// Converts the phasor to an 8-byte array.
    ///
    /// # Returns
    ///
    /// An 8-byte array containing the magnitude and angle in big-endian format.
    pub fn to_hex(&self) -> [u8; 8] {
        let mut result = [0u8; 8];
        result[0..4].copy_from_slice(&self.magnitude.to_be_bytes());
        result[4..8].copy_from_slice(&self.angle.to_be_bytes());
        result
    }
    /// Converts the phasor to floating-point rectangular format.
    ///
    /// # Returns
    ///
    /// A `PhasorFloatRect` with real and imaginary components.
    pub fn to_float_rect(&self) -> PhasorFloatRect {
        PhasorFloatRect {
            real: self.magnitude * self.angle.cos(),
            imag: self.magnitude * self.angle.sin(),
        }
    }
}

/// Represents a floating-point rectangular phasor in IEEE C37.118.
///
/// This struct stores a phasor measurement in rectangular format (real and imaginary)
/// using 32-bit floating-point values, as defined in IEEE C37.118 standards.
///
/// # Fields
///
/// * `real`: Real component in physical units.
/// * `imag`: Imaginary component in physical units.
#[derive(Debug, Clone, Copy)]
pub struct PhasorFloatRect {
    pub real: f32,
    pub imag: f32,
}
impl PhasorFloatRect {
    /// Parses a floating-point rectangular phasor from a byte slice.
    ///
    /// # Parameters
    ///
    /// * `bytes`: Byte slice containing 8 bytes (4 for real, 4 for imaginary).
    ///
    /// # Returns
    ///
    /// * `Ok(PhasorFloatRect)`: The parsed phasor.
    /// * `Err(ParseError::InvalidLength)`: If the byte slice is too short.
    pub fn from_hex(bytes: &[u8]) -> Result<Self, super::common::ParseError> {
        if bytes.len() < 8 {
            return Err(super::common::ParseError::InvalidLength {
                message: format!(
                    "Invalid length for PhasorFloatRect: expected 8 bytes, got {}",
                    bytes.len()
                ),
            });
        }

        let real = f32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let imag = f32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);

        Ok(PhasorFloatRect { real, imag })
    }

    /// Converts the phasor to an 8-byte array.
    ///
    /// # Returns
    ///
    /// An 8-byte array containing the real and imaginary components in big-endian format.
    pub fn to_hex(&self) -> [u8; 8] {
        let mut result = [0u8; 8];
        result[0..4].copy_from_slice(&self.real.to_be_bytes());
        result[4..8].copy_from_slice(&self.imag.to_be_bytes());
        result
    }
    /// Converts the phasor to floating-point polar format.
    ///
    /// # Returns
    ///
    /// A `PhasorFloatPolar` with magnitude and angle.
    pub fn to_float_polar(&self) -> PhasorFloatPolar {
        let magnitude = calc_magnitude(self.real, self.imag);
        let angle = self.imag.atan2(self.real);
        PhasorFloatPolar { magnitude, angle }
    }
}

/// Represents an integer polar phasor in IEEE C37.118.
///
/// This struct stores a phasor measurement in polar format (magnitude and angle) using
/// 16-bit integer values, requiring scaling per IEEE C37.118 standards.
///
/// # Fields
///
/// * `angle`: Raw angle value (scaled to radians when converted).
/// * `magnitude`: Raw magnitude value (scaled to physical units when converted).
#[derive(Debug, Clone, Copy)]
pub struct PhasorIntPolar {
    pub angle: i16,
    pub magnitude: u16,
}
impl PhasorIntPolar {
    /// Parses an integer polar phasor from a byte slice.
    ///
    /// # Parameters
    ///
    /// * `bytes`: Byte slice containing 4 bytes (2 for angle, 2 for magnitude).
    ///
    /// # Returns
    ///
    /// * `Ok(PhasorIntPolar)`: The parsed phasor.
    /// * `Err(ParseError::InvalidLength)`: If the byte slice is too short.
    pub fn from_hex(bytes: &[u8]) -> Result<Self, super::common::ParseError> {
        if bytes.len() < 4 {
            return Err(super::common::ParseError::InvalidLength {
                message: format!(
                    "Invalid length for PhasorIntPolar: expected 4 bytes, got {}",
                    bytes.len()
                ),
            });
        }

        // needs to be scaled to radians.
        let angle = i16::from_be_bytes([bytes[0], bytes[1]]);

        let magnitude = u16::from_be_bytes([bytes[2], bytes[3]]);

        Ok(PhasorIntPolar { angle, magnitude })
    }

    /// Converts the phasor to a 4-byte array.
    ///
    /// # Returns
    ///
    /// A 4-byte array containing the angle and magnitude in big-endian format.
    pub fn to_hex(&self) -> [u8; 4] {
        let mut result = [0u8; 4];
        result[0..2].copy_from_slice(&self.angle.to_be_bytes());
        result[2..4].copy_from_slice(&self.magnitude.to_be_bytes());
        result
    }
    /// Converts the phasor to floating-point polar format with scaling.
    ///
    /// # Parameters
    ///
    /// * `scale_factor`: PHUNIT scaling factor for magnitude conversion.
    ///
    /// # Returns
    ///
    /// A `PhasorFloatPolar` with scaled magnitude and angle in radians.
    pub fn to_float_polar(&self, scale_factor: u32) -> PhasorFloatPolar {
        let angle = (self.angle as f32) * 0.0001;
        let magnitude = scale_phasor_value(self.magnitude as f32, scale_factor);

        PhasorFloatPolar { angle, magnitude }
    }
    /// Converts the phasor to floating-point rectangular format with scaling.
    ///
    /// # Parameters
    ///
    /// * `scale_factor`: PHUNIT scaling factor for magnitude conversion.
    ///
    /// # Returns
    ///
    /// A `PhasorFloatRect` with scaled real and imaginary components
    pub fn to_float_rect(&self, scale_factor: u32) -> PhasorFloatRect {
        let angle = (self.angle as f32) * 0.0001;
        let magnitude = scale_phasor_value(self.magnitude as f32, scale_factor);

        PhasorFloatRect {
            real: magnitude * angle.cos(),
            imag: magnitude * angle.sin(),
        }
    }
}

/// Represents an integer rectangular phasor in IEEE C37.118.
///
/// This struct stores a phasor measurement in rectangular format (real and imaginary)
/// using 16-bit integer values, requiring scaling per IEEE C37.118 standards.
///
/// # Fields
///
/// * `real`: Raw real component (scaled to physical units when converted).
/// * `imag`: Raw imaginary component (scaled to physical units when converted).
#[derive(Debug, Clone, Copy)]
pub struct PhasorIntRect {
    pub real: i16,
    pub imag: i16,
}
impl PhasorIntRect {
    /// Represents an integer rectangular phasor in IEEE C37.118.
    ///
    /// This struct stores a phasor measurement in rectangular format (real and imaginary)
    /// using 16-bit integer values, requiring scaling per IEEE C37.118 standards.
    ///
    /// # Fields
    ///
    /// * `real`: Raw real component (scaled to physical units when converted).
    /// * `imag`: Raw imaginary component (scaled to physical units when converted).
    pub fn from_hex(bytes: &[u8]) -> Result<Self, super::common::ParseError> {
        if bytes.len() < 4 {
            return Err(super::common::ParseError::InvalidLength {
                message: format!(
                    "Invalid length for PhasorIntRect: expected 4 bytes, got {}",
                    bytes.len()
                ),
            });
        }

        let real = i16::from_be_bytes([bytes[0], bytes[1]]);
        let imag = i16::from_be_bytes([bytes[2], bytes[3]]);

        Ok(PhasorIntRect { real, imag })
    }

    /// Converts the phasor to a 4-byte array.
    ///
    /// # Returns
    ///
    /// A 4-byte array containing the real and imaginary components in big-endian format.
    pub fn to_hex(&self) -> [u8; 4] {
        let mut result = [0u8; 4];
        result[0..2].copy_from_slice(&self.real.to_be_bytes());
        result[2..4].copy_from_slice(&self.imag.to_be_bytes());
        result
    }

    /// Converts the phasor to floating-point polar format with scaling.
    ///
    /// # Parameters
    ///
    /// * `scale_factor`: PHUNIT scaling factor for magnitude conversion.
    ///
    /// # Returns
    ///
    /// A `PhasorFloatPolar` with scaled magnitude and angle.
    pub fn to_float_polar(&self, scale_factor: u32) -> PhasorFloatPolar {
        // scale the
        let angle = (self.imag as f32).atan2(self.real as f32);
        let magnitude = scale_phasor_value(
            calc_magnitude(self.real as f32, self.imag as f32),
            scale_factor,
        );

        PhasorFloatPolar { magnitude, angle }
    }
    /// Converts the phasor to floating-point rectangular format with scaling.
    ///
    /// # Parameters
    ///
    /// * `scale_factor`: PHUNIT scaling factor for component conversion.
    ///
    /// # Returns
    ///
    /// A `PhasorFloatRect` with scaled real and imaginary components.
    pub fn to_float_rect(&self, scale_factor: u32) -> PhasorFloatRect {
        // scale the real and imaginary parts
        PhasorFloatRect {
            real: scale_phasor_value(self.real as f32, scale_factor),
            imag: scale_phasor_value(self.imag as f32, scale_factor),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::f32::consts::PI;

    #[test]
    fn test_phasor_polar_conversions() {
        // Create a polar phasor with magnitude 1.0 and angle PI/4 (45 degrees)
        let polar = PhasorFloatPolar {
            magnitude: 1.0,
            angle: PI / 4.0,
        };

        // Convert to rectangular
        let rect = polar.to_float_rect();

        // The real and imaginary parts should be approximately sqrt(2)/2
        assert!((rect.real - 0.7071).abs() < 0.001);
        assert!((rect.imag - 0.7071).abs() < 0.001);
    }

    #[test]
    fn test_int_to_float_conversion() {
        // Create an integer polar phasor
        let int_polar = PhasorIntPolar {
            magnitude: 100, // Raw magnitude
            angle: 7854,    // Raw angle (about 45 degrees: 7854 = 31416 /4 )
        };

        // Convert with scale factor of 10
        let scale_factor = 10;
        let float_polar = int_polar.to_float_polar(scale_factor);

        // Check scaled values (magnitude should be 100 * 10 / 1e5 = 0.01)
        assert!((float_polar.magnitude - 0.01).abs() < 0.0001);
        assert!((float_polar.angle - PI / 4.0).abs() < 0.01);
    }

    #[test]

    fn test_hex_conversion() {
        // Test FloatPolar from_hex/to_hex
        let bytes = [
            0x3F, 0x80, 0x00, 0x00, // 1.0 in IEEE 754
            0x3F, 0x00, 0x00, 0x00, // 0.5 in IEEE 754
        ];

        let phasor = PhasorFloatPolar::from_hex(&bytes).unwrap();
        assert_eq!(phasor.magnitude, 1.0);
        assert_eq!(phasor.angle, 0.5);

        let hex_bytes = phasor.to_hex();
        assert_eq!(hex_bytes, bytes);

        // Test IntRectRaw from_hex/to_hex
        let int_bytes = [0x00, 0x64, 0x00, 0x32]; // real=100, imag=50
        let int_phasor = PhasorIntRect::from_hex(&int_bytes).unwrap();
        assert_eq!(int_phasor.real, 100);
        assert_eq!(int_phasor.imag, 50);

        let int_hex = int_phasor.to_hex();
        assert_eq!(int_hex, int_bytes);
    }

    #[test]
    fn test_scaled_conversions() {
        // Test IntPolarScaled with higher scale factor
        let scale_factor = 1000;
        let raw_phasor = PhasorIntPolar {
            magnitude: 500,
            angle: 15708, // 90 degrees
        };

        // Convert to float and check values
        let float_polar = raw_phasor.to_float_polar(scale_factor);

        assert!((float_polar.magnitude - 5.0).abs() < 0.001); // 500 * 1000 / 1e5 = 5.0
        assert!((float_polar.angle - PI / 2.0).abs() < 0.001); // 16384 = PI/2 in raw angle units
    }

    #[test]
    fn test_scale_phasor_value() {
        // Test case from IEEE example 1 (voltage)
        // factor value 915527
        let factor = 915527;

        // Raw phasor value 14635 (from the example)
        let raw_value = 14635.0;

        // Expected result: 134.0 kV
        let expected_value = 134.0 * 1000.0; // 134 kV = 134,000 V

        let scaled_value = scale_phasor_value(raw_value, factor);
        assert!(
            (scaled_value - expected_value).abs() < 1000.0,
            "Expected {} but got {}",
            expected_value,
            scaled_value
        );

        // Test case from IEEE example 2 (current)
        // factor value 45776
        let factor = 45776;

        // Raw phasor value 1092 (from the example)
        let raw_value = 1092.0;

        // Expected result: 500 A
        let expected_value = 500.0;

        let scaled_value = scale_phasor_value(raw_value, factor);
        assert!(
            (scaled_value - expected_value).abs() < 1.0,
            "Expected {} but got {}",
            expected_value,
            scaled_value
        );

        // Test with zero raw value
        assert_eq!(scale_phasor_value(0.0, factor), 0.0);

        // Test with zero factor (should return zero)
        assert_eq!(scale_phasor_value(raw_value, 0), 0.0);
    }
}
