use super::sparse::{
    Accumulate, Accumulator, C37118TimestampAccumulator, F32Accumulator, I16Accumulator,
    I32Accumulator, U16Accumulator,
};
use arrow::array::{ArrayRef, Float32Array, Int16Array, Int32Array, Int64Array, UInt16Array};
use arrow::buffer::{Buffer, MutableBuffer};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rayon::prelude::*;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_BUFFER_SIZE: usize = 55 * 1024;
const BATCH_SIZE: usize = 120;

#[derive(Debug, Clone)]
pub struct AccumulatorConfig {
    pub var_loc: u16,
    pub var_len: u8,
    pub var_type: DataType,
    pub name: String,
}

#[derive(Debug)]
pub struct AccumulatorManager {
    output_buffers: Vec<Arc<Mutex<MutableBuffer>>>,
    configs: Vec<AccumulatorConfig>,
    schema: Arc<Schema>,
    records: Arc<Mutex<VecDeque<(u64, RecordBatch)>>>,
    batch_index: usize,
    max_batches: usize,
    buffer_size: usize,
    batch_size: usize,
}

impl AccumulatorManager {
    pub fn new(configs: Vec<AccumulatorConfig>, max_batches: usize) -> Self {
        Self::new_with_params(configs, max_batches, MAX_BUFFER_SIZE, BATCH_SIZE)
    }

    pub fn new_with_params(
        configs: Vec<AccumulatorConfig>,
        max_batches: usize,
        buffer_size: usize,
        batch_size: usize,
    ) -> Self {
        let valid_configs: Vec<_> = configs
            .into_iter()
            .filter(|config| {
                if (config.var_loc as usize) + (config.var_len as usize) <= buffer_size {
                    true
                } else {
                    println!(
                        "WARNING: Ignoring invalid accumulator at var_loc={} with var_len={}",
                        config.var_loc, config.var_len
                    );
                    false
                }
            })
            .collect();

        if valid_configs.is_empty() {
            panic!("No valid accumulators provided!");
        }

        let fields: Vec<Field> = valid_configs
            .iter()
            .map(|config| Field::new(&config.name, config.var_type.clone(), false))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        let output_buffers: Vec<_> = valid_configs
            .iter()
            .map(|config| {
                let capacity = match config.var_type {
                    DataType::Float32 => batch_size * std::mem::size_of::<f32>(),
                    DataType::Int32 => batch_size * std::mem::size_of::<i32>(),
                    DataType::Int16 => batch_size * std::mem::size_of::<i16>(),
                    DataType::UInt16 => batch_size * std::mem::size_of::<u16>(),
                    DataType::Int64 => batch_size * std::mem::size_of::<i64>(),
                    _ => panic!("Unsupported data type: {:?}", config.var_type),
                };
                Arc::new(Mutex::new(MutableBuffer::new(capacity)))
            })
            .collect::<Vec<_>>();

        let records = Arc::new(Mutex::new(VecDeque::with_capacity(max_batches)));

        AccumulatorManager {
            output_buffers,
            configs: valid_configs,
            schema,
            records,
            batch_index: 0,
            max_batches,
            buffer_size,
            batch_size,
        }
    }

    pub fn duplicate(&self) -> Self {
        Self::new_with_params(
            self.configs.clone(),
            self.max_batches,
            self.buffer_size,
            self.batch_size,
        )
    }

    pub fn process_buffer(&mut self, data: &[u8]) -> Result<(), String> {
        if data.len() > self.buffer_size {
            return Err("Data exceeds max buffer size".to_string());
        }

        if data.is_empty() {
            return Err("No data written to buffer".to_string());
        }

        let accumulators: Vec<Accumulator> = self
            .configs
            .iter()
            .map(|config| match (config.var_type.clone(), config.var_len) {
                (DataType::UInt16, 2) => Accumulator::U16(U16Accumulator {
                    var_loc: config.var_loc,
                }),
                (DataType::Int16, 2) => Accumulator::I16(I16Accumulator {
                    var_loc: config.var_loc,
                }),
                (DataType::Int32, 4) => Accumulator::I32(I32Accumulator {
                    var_loc: config.var_loc,
                }),
                (DataType::Float32, 4) => Accumulator::F32(F32Accumulator {
                    var_loc: config.var_loc,
                }),
                (DataType::Int64, 8) => Accumulator::Timestamp(C37118TimestampAccumulator {
                    var_loc: config.var_loc,
                }),
                (var_type, var_len) => {
                    panic!(
                        "Unsupported (type, len) combination: {:?}, {}",
                        var_type, var_len
                    )
                }
            })
            .collect();

        // Process buffer in parallel
        self.output_buffers
            .par_iter()
            .zip(accumulators.par_iter())
            .for_each(|(buffer, acc)| {
                let mut buffer_guard = buffer.lock().unwrap();
                acc.accumulate(data, &mut buffer_guard);
            });

        self.batch_index += 1;

        // If we've reached batch size, save the batch
        if self.batch_index >= self.batch_size {
            self.save_batch();
            self.batch_index = 0;
        }

        Ok(())
    }

    pub fn flush_pending_batch(&mut self) {
        if self.batch_index > 0 {
            self.save_batch();
            self.batch_index = 0;
        }
    }

    fn save_batch(&self) {
        let has_data = self.output_buffers.iter().any(|buffer| {
            let buffer = buffer.lock().unwrap();
            !buffer.is_empty()
        });

        if !has_data {
            return;
        }

        // Create arrays from buffers in parallel
        let arrays: Vec<ArrayRef> = self
            .output_buffers
            .par_iter()
            .zip(self.configs.par_iter())
            .map(|(buffer_arc, config)| {
                let buffer = buffer_arc.lock().unwrap();
                let data_buffer = Buffer::from(buffer.as_slice());

                match config.var_type {
                    DataType::Float32 => {
                        let num_values = data_buffer.len() / std::mem::size_of::<f32>();
                        let values = unsafe {
                            std::slice::from_raw_parts(
                                data_buffer.as_ptr() as *const f32,
                                num_values,
                            )
                        };
                        Arc::new(Float32Array::from(values.to_vec())) as ArrayRef
                    }
                    DataType::Int32 => {
                        let num_values = data_buffer.len() / std::mem::size_of::<i32>();
                        let values = unsafe {
                            std::slice::from_raw_parts(
                                data_buffer.as_ptr() as *const i32,
                                num_values,
                            )
                        };
                        Arc::new(Int32Array::from(values.to_vec())) as ArrayRef
                    }
                    DataType::UInt16 => {
                        let num_values = data_buffer.len() / std::mem::size_of::<u16>();
                        let values = unsafe {
                            std::slice::from_raw_parts(
                                data_buffer.as_ptr() as *const u16,
                                num_values,
                            )
                        };
                        Arc::new(UInt16Array::from(values.to_vec())) as ArrayRef
                    }
                    DataType::Int16 => {
                        let num_values = data_buffer.len() / std::mem::size_of::<i16>();
                        let values = unsafe {
                            std::slice::from_raw_parts(
                                data_buffer.as_ptr() as *const i16,
                                num_values,
                            )
                        };
                        Arc::new(Int16Array::from(values.to_vec())) as ArrayRef
                    }
                    DataType::Int64 => {
                        let num_values = data_buffer.len() / std::mem::size_of::<i64>();
                        let values = unsafe {
                            std::slice::from_raw_parts(
                                data_buffer.as_ptr() as *const i64,
                                num_values,
                            )
                        };
                        Arc::new(Int64Array::from(values.to_vec())) as ArrayRef
                    }
                    _ => panic!("Unsupported data type"),
                }
            })
            .collect();

        // Create record batch
        let batch = RecordBatch::try_new(self.schema.clone(), arrays).unwrap();

        // Get timestamp
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Store in records
        let mut records = self.records.lock().unwrap();
        records.push_back((timestamp, batch));
        if records.len() > self.max_batches {
            records.pop_front();
        }

        // Clear all buffers
        self.output_buffers
            .par_iter()
            .for_each(|buffer| buffer.lock().unwrap().clear());
    }

    pub fn shutdown(mut self) {
        // Flush any pending batch before shutdown
        self.flush_pending_batch();
    }

    pub fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    pub fn get_dataframe(
        &mut self,
        columns: Option<Vec<&str>>,
        window_secs: Option<u64>,
    ) -> Result<RecordBatch, String> {
        // Flush any pending data to make sure we have the latest data
        self.flush_pending_batch();

        let column_indices = match columns {
            Some(col_names) => col_names
                .into_iter()
                .map(|name| {
                    self.schema
                        .index_of(name)
                        .map_err(|e| format!("Column '{}' not found in schema: {}", name, e))
                })
                .collect::<Result<Vec<usize>, String>>()?,
            None => (0..self.schema.fields().len()).collect(),
        };

        self._get_dataframe(&column_indices, window_secs.unwrap_or(u64::MAX))
    }

    pub fn _get_dataframe(
        &self,
        columns: &[usize],
        window_secs: u64,
    ) -> Result<RecordBatch, String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let start_time = now.saturating_sub(window_secs.saturating_mul(1000));

        let records = self.records.lock().unwrap();
        if records.is_empty() {
            let sub_schema = Arc::new(Schema::new(
                columns
                    .iter()
                    .map(|&i| self.schema.field(i).clone())
                    .collect::<Vec<Field>>(),
            ));

            let empty_arrays: Vec<ArrayRef> = columns
                .iter()
                .map(|&col_idx| match self.schema.field(col_idx).data_type() {
                    DataType::Float32 => {
                        Arc::new(Float32Array::from(Vec::<f32>::new())) as ArrayRef
                    }
                    DataType::Int32 => Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef,
                    DataType::UInt16 => Arc::new(UInt16Array::from(Vec::<u16>::new())) as ArrayRef,
                    DataType::Int16 => Arc::new(Int16Array::from(Vec::<i16>::new())) as ArrayRef,
                    DataType::Int64 => Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
                    dt => panic!("Unsupported data type: {:?}", dt),
                })
                .collect();

            return RecordBatch::try_new(sub_schema, empty_arrays)
                .map_err(|e| format!("Failed to create empty record batch: {}", e));
        }

        let start_idx = match records.binary_search_by(|(ts, _)| ts.cmp(&start_time)) {
            Ok(idx) => idx,
            Err(idx) => idx.saturating_sub(1).max(0),
        };

        let batches: Vec<&RecordBatch> = records
            .iter()
            .skip(start_idx)
            .map(|(_, batch)| batch)
            .collect();

        if batches.is_empty() {
            return Err("No data available for the requested window".to_string());
        }

        let sub_schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|&i| self.schema.field(i).clone())
                .collect::<Vec<Field>>(),
        ));

        let column_data: Vec<(usize, &DataType)> = columns
            .iter()
            .map(|&col_idx| (col_idx, self.schema.field(col_idx).data_type()))
            .collect();

        // Process columns in parallel
        let arrays_by_column: Vec<Vec<&dyn arrow::array::Array>> = column_data
            .par_iter()
            .map(|&(col_idx, _)| {
                batches
                    .iter()
                    .map(|batch| batch.column(col_idx).as_ref())
                    .collect()
            })
            .collect();

        let concatenated_arrays: Vec<ArrayRef> = arrays_by_column
            .into_par_iter()
            .zip(column_data.par_iter())
            .map(|(arrays, &(_, data_type))| {
                if arrays.is_empty() {
                    match data_type {
                        DataType::Float32 => {
                            Arc::new(Float32Array::from(Vec::<f32>::new())) as ArrayRef
                        }
                        DataType::Int32 => {
                            Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef
                        }
                        DataType::UInt16 => {
                            Arc::new(UInt16Array::from(Vec::<u16>::new())) as ArrayRef
                        }
                        DataType::Int16 => {
                            Arc::new(Int16Array::from(Vec::<i16>::new())) as ArrayRef
                        }
                        DataType::Int64 => {
                            Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef
                        }
                        _ => panic!("Unsupported data type: {:?}", data_type),
                    }
                } else {
                    arrow::compute::concat(&arrays).unwrap()
                }
            })
            .collect();

        RecordBatch::try_new(sub_schema, concatenated_arrays)
            .map_err(|e| format!("Failed to create record batch: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

    #[test]
    fn test_process_buffer_multiple_times() {
        let configs = vec![
            AccumulatorConfig {
                var_loc: 0,
                var_len: 4,
                var_type: DataType::Float32,
                name: "float_col".to_string(),
            },
            AccumulatorConfig {
                var_loc: 4,
                var_len: 2,
                var_type: DataType::UInt16,
                name: "uint16_col".to_string(),
            },
        ];

        let mut manager = AccumulatorManager::new_with_params(configs, 10, 16, 5);

        let input_buffer = vec![
            0x41, 0x20, 0x00, 0x00, // f32: 10.0 in big-endian
            0x00, 0x14, // u16: 20 in big-endian
        ];

        let n = 5;
        for _ in 0..n {
            manager.process_buffer(&input_buffer).unwrap();
        }

        // Get data frame will flush the pending batch
        let result = manager
            .get_dataframe(Some(vec!["float_col", "uint16_col"]), None)
            .unwrap();

        assert_eq!(result.num_rows(), n, "Should have {} rows", n);

        let float_col = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        for i in 0..n {
            assert_eq!(
                float_col.value(i),
                10.0,
                "Float column should have value 10.0 at index {}",
                i
            );
        }

        let uint16_col = result
            .column(1)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap();
        for i in 0..n {
            assert_eq!(
                uint16_col.value(i),
                20,
                "UInt16 column should have value 20 at index {}",
                i
            );
        }
    }

    #[test]
    fn test_empty_buffer() {
        let configs = vec![AccumulatorConfig {
            var_loc: 0,
            var_len: 4,
            var_type: DataType::Float32,
            name: "float_col".to_string(),
        }];

        let mut manager = AccumulatorManager::new_with_params(configs, 10, 16, 5);

        let empty_buffer: &[u8] = &[];
        let result = manager.process_buffer(empty_buffer);
        assert!(result.is_err(), "Empty buffer should return error");
        assert_eq!(result.unwrap_err(), "No data written to buffer",);

        let dataframe = manager.get_dataframe(None, None).unwrap();
        assert_eq!(dataframe.num_rows(), 0, "Dataframe should be empty");
    }

    #[test]
    fn test_shutdown() {
        let configs = vec![AccumulatorConfig {
            var_loc: 0,
            var_len: 4,
            var_type: DataType::Float32,
            name: "float_col".to_string(),
        }];

        let mut manager = AccumulatorManager::new_with_params(configs.clone(), 10, 16, 5);

        let input_buffer = [0x41, 0x20, 0x00, 0x00];
        // Process some data
        manager.process_buffer(&input_buffer).unwrap();

        // Shutdown should flush any pending batches
        manager.shutdown();

        // Create a new manager
        let mut new_manager = AccumulatorManager::new_with_params(configs, 10, 16, 5);
        let result = new_manager.process_buffer(&input_buffer);

        assert!(
            result.is_ok(),
            "Process buffer should succeed on a new manager"
        );
    }
    #[test]
    fn test_process_buffer_too_large() {
        let configs = vec![AccumulatorConfig {
            var_loc: 0,
            var_len: 4,
            var_type: DataType::Float32,
            name: "float_col".to_string(),
        }];
        let mut manager = AccumulatorManager::new_with_params(configs, 10, 4, 5);
        let input_buffer = vec![0u8; 5]; // Larger than buffer_size (4)
        let result = manager.process_buffer(&input_buffer);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "Data exceeds max buffer size".to_string()
        );
    }

    #[test]
    fn test_buffer_pool_recycling() {
        let configs = vec![AccumulatorConfig {
            var_loc: 0,
            var_len: 4,
            var_type: DataType::Float32,
            name: "float_col".to_string(),
        }];

        let mut manager = AccumulatorManager::new_with_params(configs, 10, 16, 3);

        // Process enough data to fill a batch
        let input_buffer = vec![0x41, 0x20, 0x00, 0x00]; // Float32 value

        // Process exactly batch_size items to trigger a full batch
        for _ in 0..3 {
            manager.process_buffer(&input_buffer).unwrap();
        }

        // This should have created a batch
        {
            let records = manager.records.lock().unwrap();
            assert_eq!(records.len(), 1, "Should have created one batch");
        }

        // Process one more to start a new batch
        manager.process_buffer(&input_buffer).unwrap();

        // Flush the pending batch
        manager.flush_pending_batch();

        // Now we should have two batches
        {
            let records = manager.records.lock().unwrap();
            assert_eq!(records.len(), 2, "Should have two batches after flush");
        }

        // Get a dataframe to make sure we can access the data
        let result = manager.get_dataframe(None, None).unwrap();
        assert_eq!(result.num_rows(), 4, "Should have 4 rows in total");
    }
}
