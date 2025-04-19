// Receive repeated fixed sized buffer over a TCP socket.
// Accumulate pieces of that buffer into individual Vec<u8>, each
// representing timeseries variables. The vec<u8> output array, will
// be fixed sized known at runtime. Since there will be one for each
// timeseries variable, we want to store them close to each other in memory
// to take advantage of cache locality and row/column cache hits in DRAM.
//

#![allow(unused)]

use crate::ieee_c37_118::phasors::{PhasorType, PhasorValue};
use arrow::buffer::MutableBuffer;
// statically declare error messages
const ERR_SLICE_LEN_2: &str = "Input slice must be exactly 2 bytes";
const ERR_SLICE_LEN_4: &str = "Input slice must be exactly 4 bytes";
//const ERR_SLICE_LEN_8: &str = "Input slice must be exactly 8 bytes";

// Types of available accumulators 2 bytes each to parse up to 65KB buffers.

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

pub trait Accumulate {
    // Each implementation of the Accumulator needs to read from an input buffer and write to an output buffer.
    fn accumulate(&self, input_buffer: &[u8], output_buffer: &mut MutableBuffer);
}

// 2 bytes of alignment.
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

#[repr(align(2))]
pub struct C37118TimestampAccumulator {
    pub var_loc: u16,
    pub time_base_ns: u32,
    // TODO: Add time_base to struct for scaling fracsec
}
impl Accumulate for C37118TimestampAccumulator {
    fn accumulate(&self, input_buffer: &[u8], output_buffer: &mut MutableBuffer) {
        let loc = self.var_loc as usize;

        // IEEE C37.118 timestamp: 8 bytes total
        // - First 4 bytes: seconds since UNIX epoch (u32, big-endian)
        // - Leap Second Indicator (u8) (skip)
        // - Last 3 bytes: fraction of a second in microseconds (u32, big-endian)
        let seconds = &input_buffer[loc..loc + 4];

        let err_msg = "slice length must be 4";
        let seconds = u32::from_be_bytes(seconds.try_into().expect(err_msg));
        // Microseconds is u24 but will pad to u32
        let microseconds = u32::from_be_bytes([
            0,
            input_buffer[loc + 5],
            input_buffer[loc + 6],
            input_buffer[loc + 7],
        ]);

        // combine into single i64 timestamp in nanoseconds
        // seconds * 1e9 + microseconds * 1e3
        let timestamp_ns =
            (seconds as i64 * 1_000_000_000) + (microseconds as i64 * self.time_base_ns as i64);
        output_buffer.extend_from_slice(&timestamp_ns.to_le_bytes());
    }
}

#[derive(Debug, Clone)]
pub struct C37118PhasorAccumulator {
    pub var_loc: u16,
    pub input_type: PhasorType,
    pub output_type: PhasorType,
    pub scale_factor: u32,
}

impl C37118PhasorAccumulator {
    // New method that takes two buffers - one for each component
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
            0x00,
            0x14, // u16: 20 in big-endian

                  // TODO add tests for timestamp accumulator and null f32 value.
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
        let time_base: u32 = u32::from_be_bytes([0x00, 0x0F, 0x42, 0x40]); // in 1_000_000 microseconds
        let time_base_ns = 1_000_000_000 / time_base; //
        assert_eq!(time_base_ns, 1_000, "time base should be 1000");

        // Data Frame Example Values
        // 9:00 AM on 6/6/2006 = 1_149_580_800

        let soc_fracsec_buff: [u8; 8] = [0x44, 0x85, 0x36, 0x00, 0x00, 0x00, 0x41, 0xB1];

        let mut output = MutableBuffer::new(16); // Space for a few values
        let timestamp_acc = C37118TimestampAccumulator {
            var_loc: 0,
            time_base_ns,
        };
        timestamp_acc.accumulate(&soc_fracsec_buff, &mut output);
        let bytes = output.as_slice();
        let timestamp_val = i64::from_le_bytes(bytes[0..8].try_into().unwrap());
        assert_eq!(
            timestamp_val, 1_149_580_800_016_817_000,
            "timestamp value should be 1_149_580_800_016_817_000"
        );
    }
}
