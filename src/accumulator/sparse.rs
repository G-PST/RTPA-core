// Receive repeated fixed sized buffer over a TCP socket.
// Accumulate pieces of that buffer into individual Vec<u8>, each
// representing timeseries variables. The vec<u8> output array, will
// be fixed sized known at runtime. Since there will be one for each
// timeseries variable, we want to store them close to each other in memory
// to take advantage of cache locality and row/column cache hits in DRAM.
//
// Assumes the buffer is always less than [u8;55*1024]
// Perhaps this buffer can be a compile time flag.
//
// We will want to assign several accumulators to a single thread
// that is pinned to a specific core. The goal being to keep the sparse accumulators
// in l1/l2 cache.
//
use arrow::buffer::MutableBuffer;

// statically declare error messages
const ERR_SLICE_LEN_2: &str = "Input slice must be exactly 2 bytes";
const ERR_SLICE_LEN_4: &str = "Input slice must be exactly 4 bytes";

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
}
impl Accumulate for C37118TimestampAccumulator {
    fn accumulate(&self, input_buffer: &[u8], output_buffer: &mut MutableBuffer) {
        let loc = self.var_loc as usize;
        let slice_partition = loc + 4;

        // IEEE C37.118 timestamp: 8 bytes total
        // - First 4 bytes: seconds since UNIX epoch (u32, big-endian)
        // - Next 4 bytes: fraction of a second in microseconds (u32, big-endian)
        let seconds = &input_buffer[loc..slice_partition];
        let fracsec = &input_buffer[slice_partition..slice_partition + 4];

        let err_msg = "slice length must be 4";
        let seconds = u32::from_be_bytes(seconds.try_into().expect(err_msg));
        let microseconds = u32::from_be_bytes(fracsec.try_into().expect(err_msg));

        // combine into single i64 timestamp in nanoseconds
        // seconds * 1e9 + microseconds * 1e3
        let timestamp_ns = (seconds as i64 * 1_000_000_000) + (microseconds as i64 * 1_000);
        output_buffer.extend_from_slice(&timestamp_ns.to_le_bytes());
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
}
