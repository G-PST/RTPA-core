// Structs for holding actual Phasor measurement data.

use std::fmt;

// The max value that all scale factors are divided by for PHUNIT. SEE IEEE C37.118-2011 TABLE 9
// 10e-5 V per unit divided by max of i16 32,
const SCALE_DENOMINATOR_INVERSE: f32 = 0.00001;

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
    //
    //
    value * SCALE_DENOMINATOR_INVERSE * factor as f32
}

fn calc_magnitude(real: f32, imag: f32) -> f32 {
    (real * real + imag * imag).sqrt()
}

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

#[derive(Debug, Clone, Copy)]
pub enum PhasorValue {
    FloatPolar(PhasorFloatPolar),
    FloatRect(PhasorFloatRect),
    IntPolar(PhasorIntPolar),
    IntRect(PhasorIntRect),
}

impl PhasorValue {
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

    pub fn get_type(&self) -> PhasorType {
        match self {
            PhasorValue::FloatPolar(_) => PhasorType::FloatPolar,
            PhasorValue::FloatRect(_) => PhasorType::FloatRect,

            PhasorValue::IntRect(_) => PhasorType::IntRect,
            PhasorValue::IntPolar(_) => PhasorType::IntPolar,
        }
    }
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

#[derive(Debug, Clone, Copy)]
pub struct PhasorFloatPolar {
    pub angle: f32,
    pub magnitude: f32,
}
impl PhasorFloatPolar {
    pub fn from_hex(bytes: &[u8]) -> Result<Self, super::common::ParseError> {
        if bytes.len() < 8 {
            return Err(super::common::ParseError::InvalidLength);
        }

        let magnitude = f32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let angle = f32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);

        Ok(PhasorFloatPolar { magnitude, angle })
    }

    pub fn to_hex(&self) -> [u8; 8] {
        let mut result = [0u8; 8];
        result[0..4].copy_from_slice(&self.magnitude.to_be_bytes());
        result[4..8].copy_from_slice(&self.angle.to_be_bytes());
        result
    }

    pub fn to_float_rect(&self) -> PhasorFloatRect {
        PhasorFloatRect {
            real: self.magnitude * self.angle.cos(),
            imag: self.magnitude * self.angle.sin(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PhasorFloatRect {
    pub real: f32,
    pub imag: f32,
}
impl PhasorFloatRect {
    pub fn from_hex(bytes: &[u8]) -> Result<Self, super::common::ParseError> {
        if bytes.len() < 8 {
            return Err(super::common::ParseError::InvalidLength);
        }

        let real = f32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let imag = f32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);

        Ok(PhasorFloatRect { real, imag })
    }

    pub fn to_hex(&self) -> [u8; 8] {
        let mut result = [0u8; 8];
        result[0..4].copy_from_slice(&self.real.to_be_bytes());
        result[4..8].copy_from_slice(&self.imag.to_be_bytes());
        result
    }

    pub fn to_float_polar(&self) -> PhasorFloatPolar {
        let magnitude = calc_magnitude(self.real, self.imag);
        let angle = self.imag.atan2(self.real);
        PhasorFloatPolar { magnitude, angle }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PhasorIntPolar {
    pub angle: i16,
    pub magnitude: u16,
}
impl PhasorIntPolar {
    pub fn from_hex(bytes: &[u8]) -> Result<Self, super::common::ParseError> {
        if bytes.len() < 4 {
            return Err(super::common::ParseError::InvalidLength);
        }

        // needs to be scaled to radians.
        let angle = i16::from_be_bytes([bytes[0], bytes[1]]);

        let magnitude = u16::from_be_bytes([bytes[2], bytes[3]]);

        Ok(PhasorIntPolar { angle, magnitude })
    }

    pub fn to_hex(&self) -> [u8; 4] {
        let mut result = [0u8; 4];
        result[0..2].copy_from_slice(&self.angle.to_be_bytes());
        result[2..4].copy_from_slice(&self.magnitude.to_be_bytes());
        result
    }
    pub fn to_float_polar(&self, scale_factor: u32) -> PhasorFloatPolar {
        let angle = (self.angle as f32) * 0.0001;
        let magnitude = scale_phasor_value(self.magnitude as f32, scale_factor);

        PhasorFloatPolar { angle, magnitude }
    }
    pub fn to_float_rect(&self, scale_factor: u32) -> PhasorFloatRect {
        let angle = (self.angle as f32) * 0.0001;
        let magnitude = scale_phasor_value(self.magnitude as f32, scale_factor);

        PhasorFloatRect {
            real: magnitude * angle.cos(),
            imag: magnitude * angle.sin(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PhasorIntRect {
    pub real: i16,
    pub imag: i16,
}
impl PhasorIntRect {
    pub fn from_hex(bytes: &[u8]) -> Result<Self, super::common::ParseError> {
        if bytes.len() < 4 {
            return Err(super::common::ParseError::InvalidLength);
        }

        let real = i16::from_be_bytes([bytes[0], bytes[1]]);
        let imag = i16::from_be_bytes([bytes[2], bytes[3]]);

        Ok(PhasorIntRect { real, imag })
    }

    pub fn to_hex(&self) -> [u8; 4] {
        let mut result = [0u8; 4];
        result[0..2].copy_from_slice(&self.real.to_be_bytes());
        result[2..4].copy_from_slice(&self.imag.to_be_bytes());
        result
    }

    pub fn to_float_polar(&self, scale_factor: u32) -> PhasorFloatPolar {
        // scale the
        let angle = (self.imag as f32).atan2(self.real as f32);
        let magnitude = scale_phasor_value(
            calc_magnitude(self.real as f32, self.imag as f32),
            scale_factor,
        );

        PhasorFloatPolar { magnitude, angle }
    }
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
