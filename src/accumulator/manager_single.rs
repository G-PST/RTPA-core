//use super::manager_trait::DataManager;

use super::sparse::{
    Accumulate, Accumulator, C37118TimestampAccumulator, F32Accumulator, I16Accumulator,
    I32Accumulator, U16Accumulator,
};
use arrow::array::{ArrayRef, Float32Array, Int16Array, Int32Array, Int64Array, UInt16Array};
use arrow::buffer::{Buffer, MutableBuffer};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use super::manager::AccumulatorConfig;

const MAX_BUFFER_SIZE: usize = 55 * 1024;
const BATCH_SIZE: usize = 120; // Rows per batch (e.g., 1s at 60 Hz)

pub struct SingleThreadedManager {
    accumulators: Vec<Accumulator>,
    output_buffers: Vec<MutableBuffer>,
    configs: Vec<AccumulatorConfig>,
    schema: Arc<Schema>,
    records: VecDeque<(u64, RecordBatch)>,
    max_batches: usize,
    batch_index: usize,
    buffer_size: usize,
    batch_size: usize,
}

impl SingleThreadedManager {
    pub fn new(configs: Vec<AccumulatorConfig>, max_batches: usize) -> Self {
        Self::new_with_params(configs, max_batches, MAX_BUFFER_SIZE, BATCH_SIZE)
    }

    pub fn new_with_params(
        configs: Vec<AccumulatorConfig>,
        max_batches: usize,
        buffer_size: usize,
        batch_size: usize,
    ) -> Self {
        // Validate configs
        let mut valid_configs = Vec::new();

        for config in configs {
            if (config.var_loc as usize) + (config.var_len as usize) <= buffer_size {
                valid_configs.push(config);
            } else {
                println!(
                    "WARNING: Ignoring invalid accumulator at var_loc={} with var_len={} because it exceeds buffer_size={}",
                    config.var_loc, config.var_len, buffer_size
                );
            }
        }

        if valid_configs.is_empty() {
            panic!("No valid accumulators provided!");
        }

        // Initialize schema
        let fields: Vec<Field> = valid_configs
            .iter()
            .map(|config| Field::new(&config.name, config.var_type.clone(), false))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        // Initialize output_buffers
        let output_buffers: Vec<MutableBuffer> = valid_configs
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
                MutableBuffer::new(capacity)
            })
            .collect();

        // Create accumulators
        let accumulators: Vec<Accumulator> = valid_configs
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
                (var_type, var_len) => panic!(
                    "Unsupported (type, len) combination: {:?}, {}",
                    var_type, var_len
                ),
            })
            .collect();

        SingleThreadedManager {
            accumulators,
            output_buffers,
            configs: valid_configs,
            schema,
            records: VecDeque::with_capacity(max_batches),
            max_batches,
            batch_index: 0,
            buffer_size,
            batch_size,
        }
    }

    // Convenience constructor with simple tuple inputs
    pub fn from_simple_configs(
        configs: Vec<(u16, u8, DataType, String)>,
        max_batches: usize,
    ) -> Self {
        let acc_configs = configs
            .into_iter()
            .map(|(var_loc, var_len, var_type, name)| AccumulatorConfig {
                var_loc,
                var_len,
                var_type,
                name,
            })
            .collect();

        Self::new(acc_configs, max_batches)
    }

    pub fn from_simple_configs_with_params(
        configs: Vec<(u16, u8, DataType, String)>,
        max_batches: usize,
        buffer_size: usize,
        batch_size: usize,
    ) -> Self {
        let acc_configs = configs
            .into_iter()
            .map(|(var_loc, var_len, var_type, name)| AccumulatorConfig {
                var_loc,
                var_len,
                var_type,
                name,
            })
            .collect();

        Self::new_with_params(acc_configs, max_batches, buffer_size, batch_size)
    }

    pub fn duplicate(&self) -> Self {
        // Create a new instance with the same configuration
        let new_manager = SingleThreadedManager::new_with_params(
            self.configs.clone(),
            self.max_batches,
            self.buffer_size,
            self.batch_size,
        );
        new_manager
    }

    pub fn process_buffer<F>(&mut self, writer: F) -> Result<(), String>
    where
        F: FnOnce(&mut Vec<u8>) -> usize,
    {
        // Create a buffer to receive the data
        let mut input_buffer = vec![0u8; self.buffer_size];
        let written = writer(&mut input_buffer);

        if written == 0 {
            return Err("No data written to buffer".to_string());
        }

        if written > self.buffer_size {
            return Err("Written data exceeds max buffer size".to_string());
        }

        // Process the data with each accumulator
        for (acc, buffer) in self.accumulators.iter().zip(self.output_buffers.iter_mut()) {
            acc.accumulate(&input_buffer[..written], buffer);
        }

        // Update batch index and check if we need to save
        self.batch_index += 1;

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

    fn save_batch(&mut self) {
        let mut has_data = false;
        for buffer in &self.output_buffers {
            if buffer.len() > 0 {
                has_data = true;
                break;
            }
        }

        if !has_data {
            println!("No data to save in any column");
            return;
        }

        // Convert MutableBuffers to ArrayRef
        let arrays: Vec<ArrayRef> = self
            .output_buffers
            .iter()
            .zip(self.configs.iter())
            .map(|(buffer, config)| {
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

        // Create RecordBatch
        let batch = RecordBatch::try_new(self.schema.clone(), arrays).unwrap();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Store in records, evict oldest if necessary
        self.records.push_back((timestamp, batch));
        if self.records.len() > self.max_batches {
            self.records.pop_front();
        }

        // Reset MutableBuffers
        for buffer in &mut self.output_buffers {
            buffer.clear();
        }
    }

    // Add accessor for the schema
    pub fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    pub fn get_dataframe(
        &mut self,
        columns: Option<Vec<&str>>, // Optional list of column names
        window_secs: Option<u64>,   // Optional time window in seconds
    ) -> Result<RecordBatch, String> {
        // Resolve column names to indices
        let column_indices = match columns {
            Some(col_names) => {
                let schema = self.schema.as_ref();
                col_names
                    .into_iter()
                    .map(|name| {
                        schema
                            .index_of(name)
                            .map_err(|e| format!("Column '{}' not found in schema: {}", name, e))
                    })
                    .collect::<Result<Vec<usize>, String>>()?
            }
            None => (0..self.schema.fields().len()).collect(), // Default to all columns
        };

        // Use the internal method with the resolved indices
        self._get_dataframe(&column_indices, window_secs.unwrap_or(u64::MAX))
    }

    fn _get_dataframe(
        &mut self,
        columns: &[usize],
        window_secs: u64,
    ) -> Result<RecordBatch, String> {
        self.flush_pending_batch();

        // Calculate time window
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let start_time = now.saturating_sub(window_secs * 1000);

        if self.records.is_empty() {
            // If no records, create an empty batch with the correct schema
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

        // Find the start index for the time window using binary search
        let start_idx = match self.records.binary_search_by(|(ts, _)| ts.cmp(&start_time)) {
            Ok(idx) => idx,
            Err(idx) => idx.saturating_sub(1).max(0),
        };

        // Collect batches within the window
        let batches: Vec<&RecordBatch> = self
            .records
            .iter()
            .skip(start_idx)
            .map(|(_, batch)| batch)
            .collect();

        if batches.is_empty() {
            return Err("No data available for the requested window".to_string());
        }

        // Create a subset schema for the requested columns
        let sub_schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|&i| self.schema.field(i).clone())
                .collect::<Vec<Field>>(),
        ));

        // Collect arrays for each column
        let mut concatenated_arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());

        for &col_idx in columns {
            let arrays: Vec<&dyn arrow::array::Array> = batches
                .iter()
                .map(|batch| batch.column(col_idx).as_ref())
                .collect();

            if arrays.is_empty() {
                match self.schema.field(col_idx).data_type() {
                    DataType::Float32 => {
                        concatenated_arrays.push(Arc::new(Float32Array::from(Vec::<f32>::new())))
                    }
                    DataType::Int32 => {
                        concatenated_arrays.push(Arc::new(Int32Array::from(Vec::<i32>::new())))
                    }
                    DataType::UInt16 => {
                        concatenated_arrays.push(Arc::new(UInt16Array::from(Vec::<u16>::new())))
                    }
                    DataType::Int16 => {
                        concatenated_arrays.push(Arc::new(Int16Array::from(Vec::<i16>::new())))
                    }
                    DataType::Int64 => {
                        concatenated_arrays.push(Arc::new(Int64Array::from(Vec::<i64>::new())))
                    }
                    dt => panic!("Unsupported data type: {:?}", dt),
                }
            } else {
                concatenated_arrays.push(arrow::compute::concat(&arrays).unwrap());
            }
        }

        // Create and return the final RecordBatch
        RecordBatch::try_new(sub_schema, concatenated_arrays)
            .map_err(|e| format!("Failed to create record batch: {}", e))
    }
}

impl DataManager for SingleThreadedManager {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn process_buffer<F>(&mut self, writer: F) -> Result<(), String>
    where
        F: FnOnce(&mut Vec<u8>) -> usize,
    {
        self.process_buffer(writer)
    }

    fn flush_pending_batch(&mut self) {
        self.flush_pending_batch();
    }

    fn get_dataframe(
        &mut self,
        columns: Option<Vec<&str>>,
        window_secs: Option<u64>,
    ) -> Result<RecordBatch, String> {
        self.get_dataframe(columns, window_secs)
    }

    fn duplicate(&self) -> Self {
        self.duplicate()
    }
}
