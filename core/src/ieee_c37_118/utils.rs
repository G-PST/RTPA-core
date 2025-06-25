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
        let hex_str: String = buffer
            .iter()
            .map(|b| format!("{:02X}", b))
            .collect::<Vec<String>>()
            .join(" ");
        log::warn!(
            "CRC Checksum Mismatch: Expected {:04X}, got {:04X}",
            calculated_crc,
            frame_crc
        );
        log::warn!("Buffer (Hex): {}", hex_str);
        return Err(ParseError::InvalidChecksum {
            message: format!(
                "CRC Checksum Mismatch: Expected {:04X}, got {:04X} ",
                calculated_crc, frame_crc
            ),
        });
    }
    Ok(())
}

// FIXME could potentially panic if system clock is before UNIX_EPOCH. Unlikely.
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

pub fn timestamp_from_hex(hex: &[u8], time_base: u32) -> i64 {
    let soc = u32::from_be_bytes([hex[0], hex[1], hex[2], hex[3]]);
    let fracsec = u32::from_be_bytes([0, hex[5], hex[6], hex[7]]);

    let timestamp =
        (soc as i64) * 1_000_000_000 + (fracsec as i64 * 1_000_000_000 / time_base as i64);
    timestamp
}

/// Finds the starting indices and potential completeness of frames in a buffer by locating sync bytes (0xAA).
///
/// IEEE C37.118 frames start with a sync byte of 0xAA. This function scans the input buffer
/// for occurrences of this sync byte followed by a valid frame type byte and returns the indices
/// where potential frames start along with whether the frame appears complete based on the frame size.
/// This is useful for splitting a buffer that may contain multiple frames and handling partial frames.
///
/// # Parameters
///
/// * `buffer`: The input byte slice to search for frame start indices.
///
/// # Returns
///
/// * `Ok(Vec<(usize, bool)>)` containing tuples of (start_index, is_complete) where start_index is the
///   position of the frame start, and is_complete indicates if the frame size matches the available data.
/// * `Err(ParseError::InvalidData)` if no valid sync bytes are found or subsequent bytes are invalid.
pub fn find_frame_starts(buffer: &[u8]) -> Result<Vec<(usize, bool)>, ParseError> {
    let mut frame_info = Vec::new();

    for i in 0..buffer.len() {
        // Check for sync byte 0xAA
        if buffer[i] == 0xAA {
            // Check if there's at least one more byte after sync to validate frame type
            if i + 1 < buffer.len() {
                // Basic check if next byte could be a valid frame type (0x01 to 0x05 usually)
                let next_byte = buffer[i + 1];
                if next_byte >= 0x01 && next_byte <= 0x05 {
                    // Check if we have enough bytes to read frame size (bytes 2 and 3)
                    if i + 3 < buffer.len() {
                        let frame_size =
                            u16::from_be_bytes([buffer[i + 2], buffer[i + 3]]) as usize;
                        let is_complete = i + frame_size <= buffer.len();
                        frame_info.push((i, is_complete));
                    } else {
                        // Not enough data to read frame size, consider it incomplete
                        frame_info.push((i, false));
                    }
                }
            }
        }
    }

    if frame_info.is_empty() {
        return Err(ParseError::InvalidFormat {
            message: "No valid frame sync bytes found in buffer".to_string(),
        });
    }

    Ok(frame_info)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

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
    #[test]
    fn test_timestamp_roundtrip_conversion() {
        // Test round-trip conversion with microsecond precision
        let time_base: u32 = 1_000_000; // microsecond precision

        // Create a known timestamp
        let seconds_since_epoch = 1_672_531_200; // 2023-01-01 00:00:00 UTC
        let microseconds = 654_321; // Distinct microsecond value for testing

        // Create IEEE C37.118 timestamp buffer manually
        let mut timestamp_buffer = [0u8; 8];

        // Convert seconds to big-endian bytes
        timestamp_buffer[0..4].copy_from_slice(&(seconds_since_epoch as u32).to_be_bytes());

        // Convert fracsec to big-endian bytes
        let fracsec = ((microseconds as u64 * time_base as u64) / 1_000_000) as u32;
        timestamp_buffer[4..8].copy_from_slice(&fracsec.to_be_bytes());

        // Parse using timestamp_from_hex
        let timestamp_ns = timestamp_from_hex(&timestamp_buffer, time_base);

        // Convert back to system time for comparison
        let reconstructed_time = UNIX_EPOCH + Duration::from_nanos(timestamp_ns as u64);

        // Extract components for verification
        let reconstructed_secs = reconstructed_time
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let reconstructed_micros = reconstructed_time
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_micros();

        // Verify seconds and microseconds match
        assert_eq!(
            reconstructed_secs, seconds_since_epoch,
            "Seconds should match after round-trip conversion"
        );

        assert_eq!(
            reconstructed_micros, microseconds,
            "Microsecond precision should be preserved in round-trip conversion"
        );

        // Verify by directly extracting from the timestamp_ns
        let extracted_micros = (timestamp_ns % 1_000_000_000) / 1000;
        assert_eq!(
            extracted_micros, microseconds as i64,
            "Microseconds extracted from nanosecond timestamp should match original value"
        );
    }

    #[test]
    fn test_now_to_hex_and_timestamp_from_hex() {
        // Test that now_to_hex and timestamp_from_hex work together correctly
        let time_base: u32 = 1_000_000; // microsecond precision

        // Get current system time for reference
        let now = SystemTime::now();
        let now_duration = now.duration_since(UNIX_EPOCH).unwrap();
        let now_secs = now_duration.as_secs();
        let now_micros = now_duration.subsec_micros();

        // Generate IEEE C37.118 timestamp buffer
        let timestamp_buffer = now_to_hex(time_base);

        // Parse using timestamp_from_hex
        let timestamp_ns = timestamp_from_hex(&timestamp_buffer, time_base);

        // Convert back to system time
        let reconstructed_time = UNIX_EPOCH + Duration::from_nanos(timestamp_ns as u64);
        let reconstructed_duration = reconstructed_time.duration_since(UNIX_EPOCH).unwrap();
        let reconstructed_secs = reconstructed_duration.as_secs();
        let reconstructed_micros = reconstructed_duration.subsec_micros();

        // Verify seconds match
        assert_eq!(
            reconstructed_secs, now_secs,
            "Seconds should match after round-trip conversion"
        );

        // Verify microseconds are close (allow small difference due to execution time)
        let micros_diff = if now_micros > reconstructed_micros {
            now_micros - reconstructed_micros
        } else {
            reconstructed_micros - now_micros
        };

        assert!(
            micros_diff < 1000, // Allow 1ms difference due to execution time between calls
            "Microsecond difference should be small, was {} µs",
            micros_diff
        );

        // Verify the specific sample from the original test case
        let sample_buffer: [u8; 8] = [
            0x44, 0x85, 0x36, 0x00, // SOC: 1_149_580_800 in big-endian
            0x00, // Leap second indicator (not used)
            0x00, 0x41, 0xB1, // FRACSEC: 16_817 in big-endian
        ];

        let sample_timestamp = timestamp_from_hex(&sample_buffer, 1_000_000);
        let expected_ns = 1_149_580_800_000_000_000 + 16_817_000;

        assert_eq!(
            sample_timestamp, expected_ns,
            "Sample timestamp should match expected value with microsecond precision"
        );

        // Extract microseconds to verify
        let sample_micros = (sample_timestamp % 1_000_000_000) / 1000;
        assert_eq!(
            sample_micros, 16_817,
            "Sample microseconds should be exactly 16,817"
        );
    }

    #[test]
    fn test_specific_microsecond_values() {
        // Test various specific microsecond values to ensure precision is maintained
        let time_base: u32 = 1_000_000; // microsecond precision
        let seconds = 1_672_531_200; // 2023-01-01 00:00:00 UTC

        // Test cases with specific microsecond values
        let test_cases = vec![
            1,       // Smallest meaningful value
            12,      // Small value
            123,     // Hundreds
            1_234,   // Thousands
            12_345,  // Ten thousands
            123_456, // Hundred thousands
            654_321, // Another six-digit number
            999_999, // Maximum microseconds
        ];

        for expected_micros in test_cases {
            // Create buffer with the specific microsecond value
            let mut buffer = [0u8; 8];

            // Set seconds
            buffer[0..4].copy_from_slice(&(seconds as u32).to_be_bytes());

            // Set fracsec
            let fracsec = ((expected_micros as u64 * time_base as u64) / 1_000_000) as u32;
            buffer[4..8].copy_from_slice(&fracsec.to_be_bytes());

            // Parse using timestamp_from_hex
            let timestamp_ns = timestamp_from_hex(&buffer, time_base);

            // Extract microseconds
            let actual_micros = (timestamp_ns % 1_000_000_000) / 1000;

            assert_eq!(
                actual_micros, expected_micros as i64,
                "Microsecond value {} should be preserved",
                expected_micros
            );
        }
    }
    #[test]
    fn test_find_frame_starts() {
        // Test a buffer with multiple frames starting with 0xAA sync byte
        let buffer = vec![
            0xAA, 0x01, 0x00, 0x0C, // First frame start, size 12 (complete)
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0xAA, 0x02, 0x00,
            0x0C, // Second frame start, size 12 (complete)
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0xAA, 0x03, 0x00,
            0x0C, // Third frame start, size 12 (incomplete)
            0x00, 0x01, 0x02, 0x03,
        ];

        let result = find_frame_starts(&buffer).unwrap();
        assert_eq!(
            result,
            vec![(0, true), (12, true), (24, false)],
            "Should find frame start indices and completeness correctly"
        );

        // Test empty buffer
        let empty_buffer = vec![];
        let empty_result = find_frame_starts(&empty_buffer);
        assert!(empty_result.is_err(), "Empty buffer should return error");

        // Test buffer with no sync bytes
        let no_sync_buffer = vec![0x01, 0x02, 0x03, 0x04];
        let no_sync_result = find_frame_starts(&no_sync_buffer);
        assert!(
            no_sync_result.is_err(),
            "Buffer with no sync bytes should return error"
        );
    }
}
