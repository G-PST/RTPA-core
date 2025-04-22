//! # IEEE C37.118 TCP Socket Data Accumulators
//!
//! This module provides accumulators for processing fixed-size IEEE C37.118 synchrophasor
//! data buffers received over a TCP socket, as defined in IEEE C37.118-2005,
//! IEEE C37.118.2-2011, and IEEE C37.118.2-2024 standards. It extracts timeseries
//! variables (e.g., phasors, timestamps) from input buffers and stores them in Arrow
//! buffers, optimizing for cache locality and efficient memory access.
//!
//! ## Key Components
//!
//! - `Accumulator`: Enum wrapping specific accumulator types for different data formats.
//! - `Accumulate`: Trait defining the accumulation behavior for input buffer parsing.
//! - `F32Accumulator`, `I32Accumulator`, `I16Accumulator`, `U16Accumulator`: Accumulators
//!   for numeric data types (float, signed/unsigned integers).
//! - `C37118TimestampAccumulator`: Accumulator for IEEE C37.118 timestamps (seconds and
//!   fractional seconds).
//! - `C37118PhasorAccumulator`: Accumulator for phasor measurements with format conversion.
//!
//! ## Usage
//!
//! This module is used to process streaming synchrophasor data from TCP sockets, parsing
//! fixed-size buffers into timeseries variables stored in Arrow buffers. It integrates
//! with the `phasors` module for phasor data handling and Arrow’s `MutableBuffer` for
//! efficient output storage, suitable for real-time power system monitoring applications.

use crate::ieee_c37_118::phasors::{PhasorType, PhasorValue};
use arrow::buffer::MutableBuffer;

// statically declare error messages
const ERR_SLICE_LEN_2: &str = "Input slice must be exactly 2 bytes";
const ERR_SLICE_LEN_4: &str = "Input slice must be exactly 4 bytes";

/// Enumerates accumulator types for IEEE C37.118 data parsing.
///
/// This enum wraps specific accumulators for different data types (e.g., floats,
/// integers, timestamps, phasors) extracted from IEEE C37.118 synchrophasor data
/// buffers, enabling efficient timeseries accumulation.
///
/// # Variants
///
/// * `U16`: Accumulator for 16-bit unsigned integers.
/// * `F32`: Accumulator for 32-bit floating-point values.
/// * `I32`: Accumulator for 32-bit signed integers.
/// * `I16`: Accumulator for 16-bit signed integers.
/// * `Timestamp`: Accumulator for IEEE C37.118 timestamps.
pub enum Accumulator {
    U16(U16Accumulator),
    F32(F32Accumulator),
    I32(I32Accumulator),
    I16(I16Accumulator),
    Timestamp(C37118TimestampAccumulator),
}

impl super::sparse::Accumulate for Accumulator {
    fn accumulate(&self, input_buffer: &[u8], output_buffer: &mut MutableBuffer) {
        match self {
            Accumulator::U16(acc) => acc.accumulate(input_buffer, output_buffer),
            Accumulator::I32(acc) => acc.accumulate(input_buffer, output_buffer),
            Accumulator::F32(acc) => acc.accumulate(input_buffer, output_buffer),
            Accumulator::I16(acc) => acc.accumulate(input_buffer, output_buffer),
            Accumulator::Timestamp(acc) => acc.accumulate(input_buffer, output_buffer),
        }
    }
}

/// Trait for accumulating data from input buffers to Arrow buffers.
///
/// Implementors of this trait parse specific data types from IEEE C37.118 input
/// buffers and append them to an Arrow `MutableBuffer` in little-endian format,
/// optimizing for cache locality in timeseries processing.
pub trait Accumulate {
    /// Accumulates data from an input buffer to an output buffer.
    ///
    /// # Parameters
    ///
    /// * `input_buffer`: The input byte slice containing IEEE C37.118 data.
    /// * `output_buffer`: The Arrow `MutableBuffer` to store parsed values.
    fn accumulate(&self, input_buffer: &[u8], output_buffer: &mut MutableBuffer);
}

/// Accumulator for 32-bit floating-point values in IEEE C37.118 data.
///
/// This struct extracts 32-bit floats from a specified location in an input buffer
/// and stores them in an Arrow buffer in little-endian format.
///
/// # Fields
///
/// * `var_loc`: The starting byte offset in the input buffer.
#[repr(align(2))]
pub struct F32Accumulator {
    pub var_loc: u16,
}
impl Accumulate for F32Accumulator {
    fn accumulate(&self, input_buffer: &[u8], output_buffer: &mut MutableBuffer) {
        // cast variable location and length to usize.
        let loc = self.var_loc as usize;
        let slice = &input_buffer[loc..loc + 4];

        // convert to f32 value and insert as little endian.
        let value = f32::from_be_bytes(slice.try_into().expect(ERR_SLICE_LEN_4));
        output_buffer.extend_from_slice(&value.to_le_bytes());
    }
}

/// Accumulator for 32-bit signed integer values in IEEE C37.118 data.
///
/// This struct extracts 32-bit signed integers from a specified location in an input
/// buffer and stores them in an Arrow buffer in little-endian format.
///
/// # Fields
///
/// * `var_loc`: The starting byte offset in the input buffer.
#[repr(align(2))]
pub struct I32Accumulator {
    pub var_loc: u16,
}
impl Accumulate for I32Accumulator {
    fn accumulate(&self, input_buffer: &[u8], output_buffer: &mut MutableBuffer) {
        // cast variable location and length to usize.
        let loc = self.var_loc as usize;
        let slice = &input_buffer[loc..loc + 4];

        // convert to i32 value and insert as little endian.
        let value = i32::from_be_bytes(slice.try_into().expect(ERR_SLICE_LEN_4));
        output_buffer.extend_from_slice(&value.to_le_bytes());
    }
}

/// Accumulator for 16-bit signed integer values in IEEE C37.118 data.
///
/// This struct extracts 16-bit signed integers from a specified location in an input
/// buffer and stores them in an Arrow buffer in little-endian format.
///
/// # Fields
///
/// * `var_loc`: The starting byte offset in the input buffer.
#[repr(align(2))]
pub struct I16Accumulator {
    pub var_loc: u16,
}
impl Accumulate for I16Accumulator {
    fn accumulate(&self, input_buffer: &[u8], output_buffer: &mut MutableBuffer) {
        // cast variable location and length to usize.
        let loc = self.var_loc as usize;
        let slice = &input_buffer[loc..loc + 2];

        // convert to i32 value and insert as little endian.
        let value = i16::from_be_bytes(slice.try_into().expect(ERR_SLICE_LEN_4));
        output_buffer.extend_from_slice(&value.to_le_bytes());
    }
}

/// Accumulator for 16-bit unsigned integer values in IEEE C37.118 data.
///
/// This struct extracts 16-bit unsigned integers from a specified location in an input
/// buffer and stores them in an Arrow buffer in little-endian format.
///
/// # Fields
///
/// * `var_loc`: The starting byte offset in the input buffer.
#[repr(align(2))]
pub struct U16Accumulator {
    pub var_loc: u16,
}
impl Accumulate for U16Accumulator {
    fn accumulate(&self, input_buffer: &[u8], output_buffer: &mut MutableBuffer) {
        let loc = self.var_loc as usize;
        let slice = &input_buffer[loc..loc + 2];

        // convert to u16 value and insert as little endian.
        let value = u16::from_be_bytes(slice.try_into().expect(ERR_SLICE_LEN_2));
        output_buffer.extend_from_slice(&value.to_le_bytes());
    }
}

/// Accumulator for IEEE C37.118 timestamps.
///
/// This struct extracts 8-byte timestamps (4-byte seconds since UNIX epoch, 3-byte
/// fractional seconds, 1-byte leap second indicator) from an input buffer and stores
/// them as 64-bit nanosecond timestamps in an Arrow buffer.
///
/// # Fields
///
/// * `var_loc`: The starting byte offset in the input buffer.
/// * `time_base_ns`: Time base for scaling fractional seconds to nanoseconds.
///
/// # Note
///
/// A TODO indicates potential addition of time base scaling to the struct.
#[repr(align(2))]
pub struct C37118TimestampAccumulator {
    pub var_loc: u16,
    pub time_base: u32,
    // TODO: Add time_base to struct for scaling fracsec
}
impl Accumulate for C37118TimestampAccumulator {
    fn accumulate(&self, input_buffer: &[u8], output_buffer: &mut MutableBuffer) {
        let loc = self.var_loc as usize;
        let time_buffer = &input_buffer[loc..loc + 8];

        // IEEE C37.118 timestamp: 8 bytes total
        // - First 4 bytes: seconds since UNIX epoch (u32, big-endian)
        // - Leap Second Indicator (u8) (skip)
        // - Last 3 bytes: fraction of a second in microseconds (u32, big-endian)

        let timestamp_ns =
            crate::ieee_c37_118::utils::timestamp_from_hex(time_buffer, self.time_base);

        // Write the timestamp as little-endian to the output buffer
        output_buffer.extend_from_slice(&timestamp_ns.to_le_bytes());
    }
}

/// Accumulator for IEEE C37.118 phasor measurements.
///
/// This struct extracts phasor data (polar or rectangular, integer or float) from an
/// input buffer, converts it to a specified output format, and stores the components
/// in separate Arrow buffers for efficient timeseries processing.
///
/// # Fields
///
/// * `var_loc`: The starting byte offset in the input buffer.
/// * `input_type`: The input phasor format (e.g., `FloatPolar`).
/// * `output_type`: The desired output phasor format.
/// * `scale_factor`: PHUNIT scaling factor for integer phasors.
#[derive(Debug, Clone)]
pub struct C37118PhasorAccumulator {
    pub var_loc: u16,
    pub input_type: PhasorType,
    pub output_type: PhasorType,
    pub scale_factor: u32,
}

impl C37118PhasorAccumulator {
    /// Accumulates phasor data into two separate Arrow buffers.
    ///
    /// # Parameters
    ///
    /// * `input_buffer`: The input byte slice containing phasor data.
    /// * `component1_buffer`: Arrow buffer for the first component (e.g., magnitude, real).
    /// * `component2_buffer`: Arrow buffer for the second component (e.g., angle, imaginary).
    ///
    /// # Panics
    ///
    /// Panics if phasor parsing fails or if scaling is required but unavailable.
    pub fn accumulate(
        &self,
        input_buffer: &[u8],
        component1_buffer: &mut MutableBuffer,
        component2_buffer: &mut MutableBuffer,
    ) {
        let loc = self.var_loc as usize;

        // Determine input size based on input type
        let input_size = match self.input_type {
            PhasorType::FloatPolar | PhasorType::FloatRect => 8, // 2 * f32
            PhasorType::IntPolar | PhasorType::IntRect => 4,
        };

        let slice = &input_buffer[loc..loc + input_size];

        // Parse the input data to the correct phasor type
        let phasor_value =
            PhasorValue::from_hex(slice, self.input_type).expect("Failed to parse phasor data");

        // Convert to the desired output type
        match self.output_type {
            PhasorType::FloatPolar => {
                let phasor_converted = phasor_value.to_float_polar(Some(self.scale_factor));
                component1_buffer.extend_from_slice(&phasor_converted.magnitude.to_le_bytes());
                component2_buffer.extend_from_slice(&phasor_converted.angle.to_le_bytes());
            }
            PhasorType::FloatRect => {
                let converted_phasor = phasor_value.to_float_rect(Some(self.scale_factor));
                component1_buffer.extend_from_slice(&converted_phasor.real.to_le_bytes());
                component2_buffer.extend_from_slice(&converted_phasor.imag.to_le_bytes());
            }
            _ => {
                // If no conversion requested, write the native format values
                match phasor_value {
                    PhasorValue::FloatPolar(phasor) => {
                        component1_buffer.extend_from_slice(&phasor.magnitude.to_le_bytes());
                        component2_buffer.extend_from_slice(&phasor.angle.to_le_bytes());
                    }
                    PhasorValue::FloatRect(phasor) => {
                        component1_buffer.extend_from_slice(&phasor.real.to_le_bytes());
                        component2_buffer.extend_from_slice(&phasor.imag.to_le_bytes());
                    }
                    PhasorValue::IntPolar(phasor) => {
                        component1_buffer.extend_from_slice(&phasor.magnitude.to_le_bytes());
                        component2_buffer.extend_from_slice(&phasor.angle.to_le_bytes());
                    }
                    PhasorValue::IntRect(phasor) => {
                        component1_buffer.extend_from_slice(&phasor.real.to_le_bytes());
                        component2_buffer.extend_from_slice(&phasor.imag.to_le_bytes());
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_accumulators() {
        let mut output = MutableBuffer::new(16); // Space for a few values
        let input = vec![
            0x41, 0x20, 0x00, 0x00, // f32: 10.0 in big-endian
            0x00, 0x00, 0x00, 0x0A, // i32: 10 in big-endian
            0x00, 0x14, // u16: 20 in big-endian
        ];

        let f32_acc = F32Accumulator { var_loc: 0 };
        let i32_acc = I32Accumulator { var_loc: 4 };
        let u16_acc = U16Accumulator { var_loc: 8 };

        f32_acc.accumulate(&input, &mut output);
        i32_acc.accumulate(&input, &mut output);
        u16_acc.accumulate(&input, &mut output);

        let bytes = output.as_slice();
        let f32_val = f32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let i32_val = i32::from_le_bytes(bytes[4..8].try_into().unwrap());
        let u16_val = u16::from_le_bytes(bytes[8..10].try_into().unwrap());

        assert_eq!(f32_val, 10.0, "f32 value should be 10.0");
        assert_eq!(i32_val, 10, "i32 value should be 10");
        assert_eq!(u16_val, 20, "u16 value should be 20");
    }

    #[test]
    fn test_timestamp_acc() {
        // IEEE C37.118-2011 Timestamp example
        //
        let time_base: u32 = u32::from_be_bytes([0x00, 0x0F, 0x42, 0x40]); // in 1_000 microseconds
        let time_base_ns = 1_000_000_000 / time_base; //
        assert_eq!(time_base_ns, 1_000, "time base should be 1000");

        // Data Frame Example Values
        // 9:00 AM on 6/6/2006 = 1_149_580_800

        let soc_fracsec_buff: [u8; 8] = [0x44, 0x85, 0x36, 0x00, 0x00, 0x00, 0x41, 0xB1];

        let mut output = MutableBuffer::new(16); // Space for a few values
        let timestamp_acc = C37118TimestampAccumulator {
            var_loc: 0,
            time_base,
        };
        timestamp_acc.accumulate(&soc_fracsec_buff, &mut output);
        let bytes = output.as_slice();
        let timestamp_val = i64::from_le_bytes(bytes[0..8].try_into().unwrap());
        assert_eq!(
            timestamp_val, 1_149_580_800_016_817_000,
            "timestamp value should be 1_149_580_800_016_817_000"
        );
        let microseconds = (timestamp_val % 1_000_000_000) / 1000;
        assert_eq!(
            microseconds, 16_817,
            "Microsecond component should be exactly 16,817"
        );
    }
    #[test]
    fn test_timestamp_accumulator_with_utils() {
        // This test verifies that C37118TimestampAccumulator correctly uses
        // the timestamp_from_hex function to parse timestamps with microsecond precision

        // Time base in units per second (1 million = microsecond precision)
        let time_base: u32 = 1_000_000;

        // Create known timestamp buffer: 2023-01-01 00:00:00.654321 UTC
        let seconds_since_epoch = 1_672_531_200;
        let microseconds = 654_321;

        let mut timestamp_buffer = [0u8; 8];

        // Set seconds
        timestamp_buffer[0..4].copy_from_slice(&(seconds_since_epoch as u32).to_be_bytes());

        // Set fracsec
        let fracsec = ((microseconds as u64 * time_base as u64) / 1_000_000) as u32;
        timestamp_buffer[4..8].copy_from_slice(&fracsec.to_be_bytes());

        // Parse using the accumulator
        let mut output = MutableBuffer::new(8);

        let timestamp_acc = C37118TimestampAccumulator {
            var_loc: 0,
            time_base,
        };

        timestamp_acc.accumulate(&timestamp_buffer, &mut output);

        // Extract the timestamp value
        let bytes = output.as_slice();
        let timestamp_ns = i64::from_le_bytes(bytes[0..8].try_into().unwrap());

        // Expected nanosecond timestamp
        let expected_ns = seconds_since_epoch as i64 * 1_000_000_000 + microseconds as i64 * 1_000;

        assert_eq!(
            timestamp_ns, expected_ns,
            "Timestamp should match the expected value with microsecond precision"
        );

        // Extract microseconds
        let actual_micros = (timestamp_ns % 1_000_000_000) / 1000;

        assert_eq!(
            actual_micros, microseconds as i64,
            "Microsecond precision should be preserved"
        );

        // Test the specific example from the original test
        let sample_buffer: [u8; 8] = [
            0x44, 0x85, 0x36, 0x00, // SOC: 1_149_580_800 in big-endian
            0x00, // Leap second indicator (not used)
            0x00, 0x41, 0xB1, // FRACSEC: 16_817 in big-endian
        ];

        output.clear();
        timestamp_acc.accumulate(&sample_buffer, &mut output);

        let sample_timestamp = i64::from_le_bytes(output.as_slice()[0..8].try_into().unwrap());
        let expected_sample_ns = 1_149_580_800_000_000_000 + 16_817_000;

        assert_eq!(
            sample_timestamp, expected_sample_ns,
            "Sample timestamp should match expected value with microsecond precision"
        );

        // Extract microseconds to verify
        let sample_micros = (sample_timestamp % 1_000_000_000) / 1000;
        assert_eq!(
            sample_micros, 16_817,
            "Sample microseconds should be exactly 16,817"
        );
    }
}
