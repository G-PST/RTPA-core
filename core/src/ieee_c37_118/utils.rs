//! # IEEE C37.118 Frame Parsing Utilities
//!
//! This module provides functions for parsing IEEE C37.118 frames, typically operating on
//! buffers or bytes and returning enums or smaller results. It includes utilities for
//! calculating and validating Cyclic Redundancy Check (CRC) checksums as specified in
//! IEEE C37.118.2-2011 Appendix B.

use super::common::ParseError;
use std::time::SystemTime;
/// Calculates the CRC-CCITT checksum for a given buffer.
///
/// This implementation follows the CRC-CCITT algorithm as specified in
/// IEEE C37.118.2-2011 Appendix B. The checksum is calculated over the input
/// buffer and returned as a 16-bit unsigned integer.
///
/// # Parameters
///
/// * `buffer`: The input byte slice to calculate the CRC for.
///
/// # Returns
///
/// The calculated 16-bit CRC checksum.

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

/// Validates the checksum of a given buffer.
///
/// Checks if the buffer's last two bytes match the calculated CRC-CCITT checksum
/// for the preceding bytes, as per IEEE C37.118.2-2011. Returns `Ok(())` if valid,
/// or an error if the checksum mismatches or the buffer is too short.
///
/// # Parameters
///
/// * `buffer`: The input byte slice, where the last two bytes are the expected CRC.
///
/// # Returns
///
/// * `Ok(())` if the checksum is valid.
/// * `Err(ParseError::InvalidLength)` if the buffer is too short.
/// * `Err(ParseError::InvalidChecksum)` if the checksum does not match.

pub fn validate_checksum(buffer: &[u8]) -> Result<(), ParseError> {
    if buffer.len() < 2 {
        return Err(ParseError::InvalidLength {
            message: format!("Buffer too short: {}", buffer.len()),
        });
    }

    let calculated_crc = calculate_crc(&buffer[..buffer.len() - 2]);
    let frame_crc = u16::from_be_bytes([buffer[buffer.len() - 2], buffer[buffer.len() - 1]]);

    if calculated_crc != frame_crc {
        return Err(ParseError::InvalidChecksum {
            message: format!(
                "CRC Checksum Mismatch: Expected {:04X}, got {:04X}",
                calculated_crc, frame_crc
            ),
        });
    }
    Ok(())
}

pub fn now_to_hex(time_base: u32) -> [u8; 8] {
    let mut buf = [0u8; 8];

    // Get current time since Unix epoch
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();

    // Get the seconds and convert to u32
    let seconds = now.as_secs() as u32;

    // Get the fraction of the seconds and convert according to time_base
    // time_base represents the number of counts per second
    let fracsec =
        ((now.subsec_nanos() as u64 * time_base as u64) / 1_000_000_000) as u32 & 0x00FFFFFF;

    // Write seconds to the first 4 bytes (big-endian)
    buf[0..4].copy_from_slice(&seconds.to_be_bytes());

    // Write fraction of seconds to the last 4 bytes (big-endian)
    buf[4..8].copy_from_slice(&fracsec.to_be_bytes());

    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_now_to_hex_time_quality_byte() {
        // Test with different time_base values to ensure 5th byte is always zero
        for time_base in [1, 10, 100, 1000, 10000, 100000] {
            let time_hex = now_to_hex(time_base);
            assert_eq!(
                time_hex[4], 0,
                "The 5th byte (time quality) should always be zero"
            );
        }
    }
}
