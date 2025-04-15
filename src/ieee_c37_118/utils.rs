// General functions for parsing IEEEC37.118 frames.
// Typically operate on buffers or bytes and return Enums or smaller results.
// CHK - Cyclic Redundancy Check // If fragmented, last two bytes of last fragement contain the CHK.
// CRC-CCITT implementation based on IEEE C37.118.2-2011 Appendix B

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

// Check if a buffer's checksum is valid
pub fn validate_checksum(buffer: &[u8]) -> bool {
    if buffer.len() < 2 {
        return false;
    }

    let calculated_crc = calculate_crc(&buffer[..buffer.len() - 2]);
    let frame_crc = u16::from_be_bytes([buffer[buffer.len() - 2], buffer[buffer.len() - 1]]);

    calculated_crc == frame_crc
}
