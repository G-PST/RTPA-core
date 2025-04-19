use arrow::datatypes::DataType;
use rtpa_core::accumulator::manager::AccumulatorConfig;

pub fn create_test_buffer(size: usize) -> Vec<u8> {
    let mut buffer = vec![0u8; size];
    for i in 0..size {
        buffer[i] = (i % 256) as u8;
    }
    buffer
}

pub fn create_configs(num_columns: usize, buffer_size: usize) -> Vec<AccumulatorConfig> {
    let mut configs = Vec::with_capacity(num_columns);

    for i in 0..num_columns {
        let var_type = match i % 3 {
            0 => DataType::Float32,
            1 => DataType::Int32,
            _ => DataType::UInt16,
        };

        let var_len = match var_type {
            DataType::Float32 => 4 as u8,
            DataType::Int32 => 4 as u8,
            DataType::UInt16 => 2 as u8,
            _ => panic!("Unsupported data type"),
        };

        // Ensure var_loc is within the buffer size
        let max_loc = buffer_size.saturating_sub(var_len as usize);
        let var_loc = ((i * 4) % max_loc) as u16;

        configs.push(AccumulatorConfig {
            var_loc,
            var_len,
            scale_factor: 1,
            var_type,
            name: format!("column_{}", i),
        });
    }

    configs
}
