// General functions for parsing IEEEC37.118 frames.
// Typically operate on buffers or bytes and return Enums or smaller results.
// CHK - Cyclic Redundancy Check // If fragmented, last two bytes of last fragement contain the CHK.
// CRC-CCITT implementation based on IEEE C37.118.2-2011 Appendix B

use super::ConfigurationFrame;
use crate::ieee_c37_118::frames::{FrameType, ParseError, VersionStandard};
use crate::ieee_c37_118::frames_v2::ConfigurationFrame1and2_2011;

pub fn calculate_crc(buffer: &[u8]) -> u16 {
    let mut crc: u16 = 0xFFFF;
    for &byte in buffer {
        crc ^= (byte as u16) << 8;
        for _ in 0..8 {
            if (crc & 0x8000) != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

// Extract frame type from sync bytes
pub fn parse_frame_type(sync: u16) -> Result<FrameType, ParseError> {
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

pub fn parse_protocol_version(sync: u16) -> Result<VersionStandard, ParseError> {
    // Extract second byte (low byte)
    let second_byte = (sync & 0xFF) as u8;

    // Check reserved bit (bit 7) is 0
    if second_byte & 0x80 != 0 {
        return Err(ParseError::InvalidHeader);
    }

    // Extract version (bits 3-0)
    let version_bits = second_byte & 0x0F;
    let version = match version_bits {
        1 => VersionStandard::Ieee2005,
        2 => VersionStandard::Ieee2011,
        3 => VersionStandard::Ieee2024,
        4..=15 => VersionStandard::Other(version_bits),
        0 => return Err(ParseError::VersionNotSupported),
        _ => unreachable!(), // Can't happen with 4 bits
    };

    Ok(version)
}

// Check if a buffer's checksum is valid
pub fn validate_checksum(buffer: &[u8]) -> bool {
    if buffer.len() < 2 {
        return false;
    }

    let calculated_crc = calculate_crc(&buffer[..buffer.len() - 2]);
    let frame_crc = u16::from_be_bytes([buffer[buffer.len() - 2], buffer[buffer.len() - 1]]);

    calculated_crc == frame_crc
}

// Helper function to parse a fixed-point value to a float
pub fn parse_fixed_point(value: i16, scale_factor: f32) -> f32 {
    value as f32 * scale_factor
}

// Helper function to parse phasor values
pub fn parse_phasor(
    bytes: &[u8],
    is_float: bool,
    is_polar: bool,
) -> Result<(f32, f32), ParseError> {
    if is_float {
        if bytes.len() < 8 {
            return Err(ParseError::InvalidLength);
        }

        let real_or_mag = f32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let imag_or_ang = f32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);

        if is_polar {
            Ok((real_or_mag, imag_or_ang)) // Already in magnitude and angle
        } else {
            // Convert rectangular to polar
            let magnitude = (real_or_mag * real_or_mag + imag_or_ang * imag_or_ang).sqrt();
            let angle = imag_or_ang.atan2(real_or_mag);
            Ok((magnitude, angle))
        }
    } else {
        if bytes.len() < 4 {
            return Err(ParseError::InvalidLength);
        }

        let real_or_mag = i16::from_be_bytes([bytes[0], bytes[1]]);
        let imag_or_ang = i16::from_be_bytes([bytes[2], bytes[3]]);

        if is_polar {
            // Convert fixed-point values to floating-point for convenience
            Ok((real_or_mag as f32, imag_or_ang as f32))
        } else {
            // Convert rectangular to polar
            let real_f = real_or_mag as f32;
            let imag_f = imag_or_ang as f32;
            let magnitude = (real_f * real_f + imag_f * imag_f).sqrt();
            let angle = imag_f.atan2(real_f);
            Ok((magnitude, angle))
        }
    }
}

// Updated tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_frame() {
        // AA41 = Data frame (000), Version 1 (0001)
        assert_eq!(
            parse_protocol_version(0xAA41).unwrap(),
            VersionStandard::Ieee2005
        );

        // AA62 = Config1 frame (010), Version 2 (0010)
        assert_eq!(
            parse_protocol_version(0xAA62).unwrap(),
            VersionStandard::Ieee2011
        );

        // AAB3 = Config3 frame (101), Version 3 (0011)
        assert_eq!(
            parse_protocol_version(0xAAB3).unwrap(),
            VersionStandard::Ieee2024
        );

        // AA80 = Invalid (reserved bit 7 set)
        assert!(matches!(
            parse_frame_type(0xAA80),
            Err(ParseError::InvalidHeader)
        ));

        // AA70 = Invalid frame type (111)
        assert!(matches!(
            parse_frame_type(0xAA70),
            Err(ParseError::InvalidFrameType)
        ));

        // AA00 = Invalid version (0000)
        assert!(matches!(
            parse_frame_type(0xAA00),
            Err(ParseError::VersionNotSupported)
        ));
    }
}
