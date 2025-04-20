// SPDX-License-Identifier: BSD-3-Clause
//! # IEEE C37.118 Frame Parsing Utilities
//!
//! This module provides functions for parsing IEEE C37.118 frames, typically operating on
//! buffers or bytes and returning enums or smaller results. It includes utilities for
//! calculating and validating Cyclic Redundancy Check (CRC) checksums as specified in
//! IEEE C37.118.2-2011 Appendix B.
//!
//! ## Copyright and Authorship
//!
//! Copyright (c) 2025 Alliance for Sustainable Energy, LLC.
//! Developed by Micah Webb at the National Renewable Energy Laboratory (NREL).
//! Licensed under the BSD 3-Clause License. See the `LICENSE` file for details.

use super::common::ParseError;

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
///
/// # Examples
///
/// ```
/// let data = [0xAA, 0xBB, 0xCC];
/// let crc = calculate_crc(&data);
/// assert_eq!(crc, some_expected_value);
/// ```
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
///
/// # Examples
///
/// ```
/// let valid_buffer = [0xAA, 0xBB, 0xCC, 0x12, 0x34]; // Last two bytes are CRC
/// assert!(validate_checksum(&valid_buffer).is_ok());
///
/// let invalid_buffer = [0xAA, 0xBB, 0xCC, 0xFF, 0xFF];
/// assert!(validate_checksum(&invalid_buffer).is_err());
/// ```
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
