//! # IEEE C37.118 Accumulator Manager for Timeseries Data
//!
//! This module provides an `AccumulatorManager` for orchestrating accumulators to process
//! IEEE C37.118 synchrophasor data buffers into Arrow record batches, as defined in
//! IEEE C37.118-2005, IEEE C37.118.2-2011, and IEEE C37.118.2-2024 standards. It manages
//! regular and phasor accumulators to extract timeseries variables (e.g., frequencies,
//! phasors) and store them efficiently for analysis.
//!
//! ## Key Components
//!
//! - `AccumulatorManager`: Manages accumulators, buffers, and record batch creation.
//! - `AccumulatorConfig`: Configuration for regular accumulators (e.g., floats, integers).
//! - `PhasorAccumulatorConfig`: Configuration for phasor accumulators with format conversion.
//!
//! ## Usage
//!
//! This module is used to process streaming synchrophasor data from TCP sockets, accumulating
//! measurements into Arrow record batches for timeseries analysis. It integrates with the
//! `sparse` module’s accumulators, `phasors` module for phasor handling, and Arrow for
//! data storage, leveraging parallel processing for efficiency in power system monitoring.

use super::sparse::{
    Accumulate, C37118PhasorAccumulator, C37118TimestampAccumulator, F32Accumulator,
    I16Accumulator, I32Accumulator, U16Accumulator,
};
use crate::ieee_c37_118::phasors::PhasorType;
use arrow::array::{
    ArrayRef, Float32Array, Int16Array, Int32Array, TimestampNanosecondArray, UInt16Array,
};
use arrow::buffer::{Buffer, MutableBuffer};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use rayon::prelude::*;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_BUFFER_SIZE: usize = u32::MAX as usize;
const BATCH_SIZE: usize = 120;

/// Configuration for regular accumulators in IEEE C37.118 data processing.
///
/// This struct defines the parameters for accumulating a single timeseries variable
/// (e.g., frequency, status) from an IEEE C37.118 data buffer.
///
/// # Fields
///
/// * `var_loc`: Starting byte offset in the input buffer.
/// * `var_len`: Length of the variable in bytes.
/// * `scale_factor`: Scaling factor for the variable (e.g., for timestamps).
/// * `var_type`: Arrow data type (e.g., `Float32`, `Int64`).
/// * `name`: Column name for the output schema.
#[derive(Debug, Clone)]
pub struct AccumulatorConfig {
    pub var_loc: u16,
    pub var_len: u8,
    pub scale_factor: u32,
    pub var_type: DataType, // Data type determins the size of the buffer slice
    pub name: String,
}

/// Configuration for phasor accumulators in IEEE C37.118 data processing.
///
/// This struct defines the parameters for accumulating phasor measurements, including
/// input and output formats, from an IEEE C37.118 data buffer.
///
/// # Fields
///
/// * `var_loc`: Starting byte offset in the input buffer.
/// * `input_type`: Input phasor format (e.g., `FloatPolar`).
/// * `output_type`: Desired output phasor format.
/// * `scale_factor`: PHUNIT scaling factor for integer phasors.
/// * `name_real`: Column name for the first component (e.g., magnitude, real).
/// * `name_imag`: Column name for the second component (e.g., angle, imaginary).
#[derive(Debug, Clone)]
pub struct PhasorAccumulatorConfig {
    pub var_loc: u16,
    pub input_type: PhasorType,
    pub output_type: PhasorType,
    pub scale_factor: u32,
    pub name_real: String,
    pub name_imag: String,
}

/// Manages accumulators for processing IEEE C37.118 synchrophasor data into Arrow record batches.
///
/// This struct orchestrates regular and phasor accumulators to extract timeseries variables
/// from input buffers, store them in Arrow buffers, and produce record batches for analysis,
/// optimizing for efficiency with parallel processing and thread-safe buffer management.
///
/// # Fields
///
/// * `output_buffers`: Thread-safe Arrow buffers for each variable.
/// * `configs`: Configurations for regular accumulators.
/// * `phasor_configs`: Configurations for phasor accumulators.
/// * `phasor_buffer_indices`: Indices of buffers for phasor components.
/// * `schema`: Arrow schema defining the output record batch structure.
/// * `records`: Thread-safe queue of timestamped record batches.
/// * `batch_index`: Current number of processed buffers in the batch.
/// * `max_batches`: Maximum number of batches to retain.
/// * `buffer_size`: Maximum input buffer size (default 65KB).
/// * `batch_size`: Number of buffers per batch (default 120).
#[derive(Debug)]
pub struct AccumulatorManager {
    output_buffers: Vec<Arc<Mutex<MutableBuffer>>>,
    configs: Vec<AccumulatorConfig>,
    phasor_configs: Vec<PhasorAccumulatorConfig>,
    phasor_buffer_indices: Vec<(usize, usize)>, // (component1_idx, component2_idx)

    schema: Arc<Schema>,
    records: Arc<Mutex<VecDeque<(u64, RecordBatch)>>>,
    batch_index: usize,
    max_batches: usize,
    buffer_size: usize,
    batch_size: usize,
}

impl AccumulatorManager {
    /// Creates a new manager with default buffer and batch sizes.
    ///
    /// # Parameters
    ///
    /// * `configs`: Configurations for regular accumulators.
    /// * `phasor_configs`: Configurations for phasor accumulators.
    /// * `max_batches`: Maximum number of record batches to retain.
    ///
    /// # Returns
    ///
    /// A new `AccumulatorManager` instance.
    ///
    /// # Panics
    ///
    /// Panics if no valid configurations are provided or if unsupported data types are used.
    pub fn new(
        configs: Vec<AccumulatorConfig>,
        phasor_configs: Vec<PhasorAccumulatorConfig>,
        max_batches: usize,
    ) -> Self {
        Self::new_with_params(
            configs,
            phasor_configs,
            max_batches,
            MAX_BUFFER_SIZE,
            BATCH_SIZE,
        )
    }

    /// Creates a new manager with custom buffer and batch sizes.
    ///
    /// # Parameters
    ///
    /// * `configs`: Configurations for regular accumulators.
    /// * `phasor_configs`: Configurations for phasor accumulators.
    /// * `max_batches`: Maximum number of record batches to retain.
    /// * `buffer_size`: Maximum input buffer size.
    /// * `batch_size`: Number of buffers per batch.
    ///
    /// # Returns
    ///
    /// A new `AccumulatorManager` instance.
    ///
    /// # Panics
    ///
    /// Panics if no valid configurations are provided or if unsupported data types are used.
    pub fn new_with_params(
        configs: Vec<AccumulatorConfig>,
        phasor_configs: Vec<PhasorAccumulatorConfig>,
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

        let valid_phasor_configs: Vec<_> = phasor_configs
            .into_iter()
            .filter(|config| {
                let input_size = match config.input_type {
                    PhasorType::FloatPolar | PhasorType::FloatRect => 8, // 2 * f32
                    _ => 4,                                              // 2 * i16/u16
                };

                if (config.var_loc as usize) + input_size <= buffer_size {
                    true
                } else {
                    println!(
                        "WARNING: Ignoring invalid phasor accumulator at var_loc={} with size={}",
                        config.var_loc, input_size
                    );
                    false
                }
            })
            .collect();

        if valid_configs.is_empty() && valid_phasor_configs.is_empty() {
            panic!("No valid accumulators provided!");
        }

        // Create fields for schema
        let mut fields = Vec::new();

        // Add fields for regular accumulators
        for config in &valid_configs {
            fields.push(Field::new(&config.name, config.var_type.clone(), false));
        }

        // Add fields for phasor components
        for config in &valid_phasor_configs {
            // Add fields based on output type
            match config.output_type {
                PhasorType::FloatRect | PhasorType::FloatPolar => {
                    fields.push(Field::new(&config.name_real, DataType::Float32, false));
                    fields.push(Field::new(&config.name_imag, DataType::Float32, false));
                }
                PhasorType::IntRect => {
                    fields.push(Field::new(&config.name_real, DataType::Int16, false));
                    fields.push(Field::new(&config.name_imag, DataType::Int16, false));
                }
                PhasorType::IntPolar => {
                    fields.push(Field::new(&config.name_real, DataType::UInt16, false));
                    fields.push(Field::new(&config.name_imag, DataType::Int16, false));
                }
            }
        }

        let schema = Arc::new(Schema::new(fields));

        // Create output buffers
        let mut output_buffers = Vec::new();

        // Buffers for regular accumulators
        for config in &valid_configs {
            let capacity = match &config.var_type {
                DataType::Float32 => batch_size * std::mem::size_of::<f32>(),
                DataType::Int32 => batch_size * std::mem::size_of::<i32>(),
                DataType::Int16 => batch_size * std::mem::size_of::<i16>(),
                DataType::UInt16 => batch_size * std::mem::size_of::<u16>(),
                DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                    batch_size * std::mem::size_of::<i64>()
                }
                _ => panic!("Unsupported data type: {:?}", config.var_type),
            };
            output_buffers.push(Arc::new(Mutex::new(MutableBuffer::new(capacity))));
        }

        // Track buffer indices for phasor components
        let mut phasor_buffer_indices = Vec::new();
        let buffer_start_idx = output_buffers.len();

        // Add buffers for phasor components
        for (i, config) in valid_phasor_configs.iter().enumerate() {
            // Create buffer sizes based on output type
            let (comp1_size, comp2_size) = match config.output_type {
                PhasorType::FloatRect | PhasorType::FloatPolar => (
                    batch_size * std::mem::size_of::<f32>(),
                    batch_size * std::mem::size_of::<f32>(),
                ),
                PhasorType::IntRect => (
                    batch_size * std::mem::size_of::<i16>(),
                    batch_size * std::mem::size_of::<i16>(),
                ),
                PhasorType::IntPolar => (
                    batch_size * std::mem::size_of::<u16>(),
                    batch_size * std::mem::size_of::<i16>(),
                ),
            };

            // Create two buffers for the phasor components
            output_buffers.push(Arc::new(Mutex::new(MutableBuffer::new(comp1_size))));
            output_buffers.push(Arc::new(Mutex::new(MutableBuffer::new(comp2_size))));

            // Store the indices of these two buffers
            let idx1 = buffer_start_idx + i * 2;
            let idx2 = buffer_start_idx + i * 2 + 1;
            phasor_buffer_indices.push((idx1, idx2));
        }

        let records = Arc::new(Mutex::new(VecDeque::with_capacity(max_batches)));

        AccumulatorManager {
            output_buffers,
            configs: valid_configs,
            phasor_configs: valid_phasor_configs,
            phasor_buffer_indices,
            schema,
            records,
            batch_index: 0,
            max_batches,
            buffer_size,
            batch_size,
        }
    }

    /// Creates a new manager with the same configurations.
    ///
    /// # Returns
    ///
    /// A new `AccumulatorManager` with the same configurations but fresh buffers and records.
    pub fn duplicate(&self) -> Self {
        Self::new_with_params(
            self.configs.clone(),
            self.phasor_configs.clone(),
            self.max_batches,
            self.buffer_size,
            self.batch_size,
        )
    }

    /// Processes an input buffer using configured accumulators.
    ///
    /// Accumulates data into output buffers, creating a record batch when the batch size is
    /// reached. Uses thread-safe operations and parallel processing for efficiency.
    ///
    /// # Parameters
    ///
    /// * `data`: Input buffer containing IEEE C37.118 data.
    ///
    /// # Returns
    ///
    /// * `Ok(())`: If processing succeeds.
    /// * `Err(String)`: If the buffer is empty, too large, or contains unsupported data types.
    pub fn process_buffer(&mut self, data: &[u8]) -> Result<(), String> {
        if data.len() > self.buffer_size {
            return Err("Data exceeds max buffer size".to_string());
        }

        if data.is_empty() {
            return Err("No data written to buffer".to_string());
        }

        // Process regular accumulators
        for (i, config) in self.configs.iter().enumerate() {
            let buffer = &self.output_buffers[i];
            let mut buffer_guard = buffer.lock().unwrap();

            match config.var_type {
                DataType::UInt16 => {
                    let acc = U16Accumulator {
                        var_loc: config.var_loc,
                    };
                    acc.accumulate(data, &mut buffer_guard);
                }
                DataType::Int16 => {
                    let acc = I16Accumulator {
                        var_loc: config.var_loc,
                    };
                    acc.accumulate(data, &mut buffer_guard);
                }
                DataType::Int32 => {
                    let acc = I32Accumulator {
                        var_loc: config.var_loc,
                    };
                    acc.accumulate(data, &mut buffer_guard);
                }
                DataType::Float32 => {
                    let acc = F32Accumulator {
                        var_loc: config.var_loc,
                    };
                    acc.accumulate(data, &mut buffer_guard);
                }
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    let acc = C37118TimestampAccumulator {
                        var_loc: config.var_loc,
                        time_base: config.scale_factor,
                    };
                    acc.accumulate(data, &mut buffer_guard);
                }
                _ => return Err(format!("Unsupported data type: {:?}", config.var_type)),
            }
        }

        // Process phasor accumulators
        for (i, config) in self.phasor_configs.iter().enumerate() {
            let (buf1_idx, buf2_idx) = self.phasor_buffer_indices[i];

            let buffer1 = &self.output_buffers[buf1_idx];
            let buffer2 = &self.output_buffers[buf2_idx];

            let mut buf1_guard = buffer1.lock().unwrap();
            let mut buf2_guard = buffer2.lock().unwrap();

            let phasor_acc = C37118PhasorAccumulator {
                var_loc: config.var_loc,
                input_type: config.input_type,
                output_type: config.output_type,
                scale_factor: config.scale_factor,
            };

            phasor_acc.accumulate(data, &mut buf1_guard, &mut buf2_guard);
        }

        self.batch_index += 1;

        // If we've reached batch size, save the batch
        if self.batch_index >= self.batch_size {
            self.save_batch();
            self.batch_index = 0;
        }

        Ok(())
    }

    /// Flushes any pending batch to a record batch.
    ///
    /// Saves the current batch to the records queue if it contains data, clearing the buffers.
    pub fn flush_pending_batch(&mut self) {
        if self.batch_index > 0 {
            self.save_batch();
            self.batch_index = 0;
        }
    }

    /// Saves the current batch to the records queue.
    ///
    /// Creates a record batch from the output buffers, adds a timestamp, and stores it in the
    /// records queue, clearing the buffers afterward.
    fn save_batch(&self) {
        let has_data = self.output_buffers.iter().any(|buffer| {
            let buffer = buffer.lock().unwrap();
            !buffer.is_empty()
        });

        if !has_data {
            return;
        }

        // Create arrays from buffers
        let arrays: Vec<ArrayRef> = self
            .output_buffers
            .iter()
            .enumerate()
            .map(|(idx, buffer_arc)| {
                let buffer = buffer_arc.lock().unwrap();
                let data_buffer = Buffer::from(buffer.as_slice());

                // Get the field data type from the schema
                let data_type = self.schema.field(idx).data_type();

                match data_type {
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
                    DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                        let num_values = data_buffer.len() / std::mem::size_of::<i64>();
                        let values = unsafe {
                            std::slice::from_raw_parts(
                                data_buffer.as_ptr() as *const i64,
                                num_values,
                            )
                        };
                        Arc::new(TimestampNanosecondArray::from(values.to_vec())) as ArrayRef
                    }
                    _ => panic!("Unsupported data type: {:?}", data_type),
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
            .iter()
            .for_each(|buffer| buffer.lock().unwrap().clear());
    }

    /// Shuts down the manager, flushing any pending batch.
    ///
    /// Ensures all accumulated data is saved before the manager is dropped.
    pub fn shutdown(mut self) {
        // Flush any pending batch before shutdown
        self.flush_pending_batch();
    }

    /// Retrieves the Arrow schema for the output record batches.
    ///
    /// # Returns
    ///
    /// An `Arc<Schema>` defining the structure of the output data.
    pub fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
    /// Retrieves a record batch with selected columns and time window.
    ///
    /// Flushes any pending batch and returns a concatenated record batch for the specified
    /// columns and time window.
    ///
    /// # Parameters
    ///
    /// * `columns`: Optional list of column names to include (all columns if `None`).
    /// * `window_secs`: Optional time window in seconds (all data if `None`).
    ///
    /// # Returns
    ///
    /// * `Ok(RecordBatch)`: The concatenated record batch.
    /// * `Err(String)`: If a column is not found or batch creation fails.
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

    /// Internal method to retrieve a record batch with specified columns and time window.
    ///
    /// # Parameters
    ///
    /// * `columns`: Indices of columns to include.
    /// * `window_secs`: Time window in seconds.
    ///
    /// # Returns
    ///
    /// * `Ok(RecordBatch)`: The concatenated record batch.
    /// * `Err(String)`: If batch creation fails or no data is available.
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
                    DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                        Arc::new(TimestampNanosecondArray::from(Vec::<i64>::new())) as ArrayRef
                    }
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
                        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                            Arc::new(TimestampNanosecondArray::from(Vec::<i64>::new())) as ArrayRef
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
                scale_factor: 1,
                name: "float_col".to_string(),
            },
            AccumulatorConfig {
                var_loc: 4,
                var_len: 2,
                var_type: DataType::UInt16,
                scale_factor: 1,
                name: "uint16_col".to_string(),
            },
        ];

        let mut manager = AccumulatorManager::new_with_params(configs, vec![], 10, 16, 5);

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
            scale_factor: 1,
            name: "float_col".to_string(),
        }];

        let mut manager = AccumulatorManager::new_with_params(configs, vec![], 10, 16, 5);

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
            scale_factor: 1,
            name: "float_col".to_string(),
        }];

        let mut manager = AccumulatorManager::new_with_params(configs.clone(), vec![], 10, 16, 5);

        let input_buffer = [0x41, 0x20, 0x00, 0x00];
        // Process some data
        manager.process_buffer(&input_buffer).unwrap();

        // Shutdown should flush any pending batches
        manager.shutdown();

        // Create a new manager
        let mut new_manager = AccumulatorManager::new_with_params(configs, vec![], 10, 16, 5);
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
            scale_factor: 1,
            name: "float_col".to_string(),
        }];
        let mut manager = AccumulatorManager::new_with_params(configs, vec![], 10, 4, 5);
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
            scale_factor: 1,
            name: "float_col".to_string(),
        }];

        let mut manager = AccumulatorManager::new_with_params(configs, vec![], 10, 16, 3);

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

    #[test]
    fn test_phasor_accumulation() {
        // Create a regular accumulator config
        let configs = vec![AccumulatorConfig {
            var_loc: 0,
            var_len: 4,
            var_type: DataType::Float32,
            scale_factor: 1,
            name: "frequency".to_string(),
        }];

        // Create a phasor accumulator config
        let phasor_configs = vec![PhasorAccumulatorConfig {
            var_loc: 4,
            input_type: PhasorType::FloatRect,
            output_type: PhasorType::FloatRect,
            scale_factor: 1,
            name_real: "voltage_real".to_string(),
            name_imag: "voltage_imag".to_string(),
        }];

        let mut manager = AccumulatorManager::new_with_params(
            configs,
            phasor_configs,
            10, // max_batches
            16, // buffer_size
            3,  // batch_size
        );

        // Create input buffer with a frequency value and a rectangular phasor
        let input_buffer = vec![
            0x41, 0x20, 0x00, 0x00, // 10.0 in IEEE 754 big-endian (frequency)
            0x3F, 0x80, 0x00, 0x00, // 1.0 in IEEE 754 big-endian (real)
            0x3F, 0x00, 0x00, 0x00, // 0.5 in IEEE 754 big-endian (imag)
        ];

        // Process the buffer multiple times
        for _ in 0..3 {
            manager.process_buffer(&input_buffer).unwrap();
        }

        // Get dataframe with all columns
        let result = manager.get_dataframe(None, None).unwrap();

        assert_eq!(result.num_rows(), 3, "Should have 3 rows");
        assert_eq!(result.num_columns(), 3, "Should have 3 columns");

        // Check the frequency column
        let freq_col = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        for i in 0..3 {
            assert_eq!(freq_col.value(i), 10.0, "Frequency should be 10.0");
        }

        // Check the phasor components
        let real_col = result
            .column(1)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        let imag_col = result
            .column(2)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();

        for i in 0..3 {
            assert_eq!(real_col.value(i), 1.0, "Real part should be 1.0");
            assert_eq!(imag_col.value(i), 0.5, "Imaginary part should be 0.5");
        }
    }

    #[ignore = "Already tested in phasors.rs"]
    #[test]
    fn test_phasor_conversion() {
        // Test phasor conversion from integer to float
        let phasor_configs = vec![PhasorAccumulatorConfig {
            var_loc: 0,
            input_type: PhasorType::IntRect,    // int16 input
            output_type: PhasorType::FloatRect, // float32 output
            scale_factor: 100,                  // Scale factor for conversion
            name_real: "voltage_real".to_string(),
            name_imag: "voltage_imag".to_string(),
        }];

        let mut manager = AccumulatorManager::new_with_params(
            vec![], // No regular configs
            phasor_configs,
            10,
            8,
            2,
        );

        // Create input buffer with int16 rectangular components
        let input_buffer = vec![
            0xE3, 0x6A, // 300 in big-endian (real) - will be scaled to 3.0
            0xCE, 0x7C, // 100 in big-endian (imag) - will be scaled to 1.0
        ];

        manager.process_buffer(&input_buffer).unwrap();
        manager.flush_pending_batch();

        let result = manager.get_dataframe(None, None).unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.num_columns(), 2);

        // Check that values were properly converted to float
        let real_col = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        let imag_col = result
            .column(1)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();

        assert_eq!(
            real_col.value(0),
            3.0,
            "Real part should be 3.0 after scaling"
        );
        assert_eq!(
            imag_col.value(0),
            1.0,
            "Imaginary part should be 1.0 after scaling"
        );
    }

    #[ignore = "Already tested in phasors.rs"]
    #[test]
    fn test_polar_phasor() {
        // Test polar phasor handling
        let phasor_configs = vec![PhasorAccumulatorConfig {
            var_loc: 0,
            input_type: PhasorType::IntPolar, // int polar input (magnitude: uint16, angle: int16)
            output_type: PhasorType::FloatPolar, // float polar output
            scale_factor: 10,
            name_real: "magnitude".to_string(),
            name_imag: "angle".to_string(),
        }];

        let mut manager = AccumulatorManager::new_with_params(
            vec![], // No regular configs
            phasor_configs,
            10,
            8,
            2,
        );

        // Create input buffer with int polar components
        let input_buffer = vec![
            0x00, 0x64, // 100 in big-endian (magnitude)
            0x00, 0x5A, // 90 in big-endian (angle in degrees)
        ];

        manager.process_buffer(&input_buffer).unwrap();
        manager.flush_pending_batch();

        let result = manager.get_dataframe(None, None).unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.num_columns(), 2);

        // Check the polar components
        let mag_col = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        let ang_col = result
            .column(1)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();

        assert_eq!(
            mag_col.value(0),
            10.0,
            "Magnitude should be 10.0 after scaling"
        );
        assert_eq!(ang_col.value(0), 90.0, "Angle should be 90.0 degrees");
    }

    #[test]
    fn test_only_phasor_configs() {
        // Test initialization with only phasor configs
        let phasor_configs = vec![
            PhasorAccumulatorConfig {
                var_loc: 0,
                input_type: PhasorType::FloatRect,
                output_type: PhasorType::FloatRect,
                scale_factor: 1,
                name_real: "phasor1_real".to_string(),
                name_imag: "phasor1_imag".to_string(),
            },
            PhasorAccumulatorConfig {
                var_loc: 8,
                input_type: PhasorType::FloatRect,
                output_type: PhasorType::FloatRect,
                scale_factor: 1,
                name_real: "phasor2_real".to_string(),
                name_imag: "phasor2_imag".to_string(),
            },
        ];

        let mut manager = AccumulatorManager::new_with_params(
            vec![], // No regular configs
            phasor_configs,
            10,
            16,
            2,
        );

        let input_buffer = vec![
            // First phasor
            0x3F, 0x80, 0x00, 0x00, // 1.0 in IEEE 754 big-endian (real)
            0x3F, 0x00, 0x00, 0x00, // 0.5 in IEEE 754 big-endian (imag)
            // Second phasor
            0x3F, 0xA0, 0x00, 0x00, // 1.25 in IEEE 754 big-endian (real)
            0x3F, 0x40, 0x00, 0x00, // 0.75 in IEEE 754 big-endian (imag)
        ];

        manager.process_buffer(&input_buffer).unwrap();
        manager.flush_pending_batch();

        let result = manager.get_dataframe(None, None).unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(
            result.num_columns(),
            4,
            "Should have 4 columns (2 phasors * 2 components)"
        );

        // Check first phasor
        let real1_col = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        let imag1_col = result
            .column(1)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(real1_col.value(0), 1.0);
        assert_eq!(imag1_col.value(0), 0.5);

        // Check second phasor
        let real2_col = result
            .column(2)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        let imag2_col = result
            .column(3)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(real2_col.value(0), 1.25);
        assert_eq!(imag2_col.value(0), 0.75);
    }

    #[test]
    fn test_mixed_regular_and_phasor_configs() {
        // Test with both regular and phasor configs
        let configs = vec![
            AccumulatorConfig {
                var_loc: 0,
                var_len: 4,
                var_type: DataType::Float32,
                scale_factor: 1,
                name: "frequency".to_string(),
            },
            AccumulatorConfig {
                var_loc: 4,
                var_len: 2,
                var_type: DataType::Int16,
                scale_factor: 1,
                name: "status".to_string(),
            },
        ];

        let phasor_configs = vec![PhasorAccumulatorConfig {
            var_loc: 6,
            input_type: PhasorType::FloatRect,
            output_type: PhasorType::FloatRect,
            scale_factor: 10,
            name_real: "voltage_real".to_string(),
            name_imag: "voltage_imag".to_string(),
        }];

        let mut manager = AccumulatorManager::new_with_params(configs, phasor_configs, 10, 16, 2);

        let input_buffer = vec![
            // Regular values
            0x41, 0x20, 0x00, 0x00, // 10.0 in IEEE 754 big-endian (frequency)
            0x00, 0x01, // 1 in big-endian (status)
            // Phasor
            0x40, 0x00, 0x00, 0x00, // 2.0 in IEEE 754 big-endian (real)
            0x40, 0x40, 0x00, 0x00, // 3.0 in IEEE 754 big-endian (imag)
        ];

        manager.process_buffer(&input_buffer).unwrap();
        manager.flush_pending_batch();

        let result = manager.get_dataframe(None, None).unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(
            result.num_columns(),
            4,
            "Should have 4 columns (2 regular + 2 phasor components)"
        );

        // Check regular columns
        let freq_col = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        let status_col = result
            .column(1)
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap();
        assert_eq!(freq_col.value(0), 10.0);
        assert_eq!(status_col.value(0), 1);

        // Check phasor components
        let real_col = result
            .column(2)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        let imag_col = result
            .column(3)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(real_col.value(0), 2.0);
        assert_eq!(imag_col.value(0), 3.0);
    }

    #[test]
    fn test_duplicate_manager() {
        // Test the duplicate method with phasor configs
        let configs = vec![AccumulatorConfig {
            var_loc: 0,
            var_len: 4,
            var_type: DataType::Float32,
            scale_factor: 1,
            name: "value".to_string(),
        }];

        let phasor_configs = vec![PhasorAccumulatorConfig {
            var_loc: 4,
            input_type: PhasorType::FloatRect,
            output_type: PhasorType::FloatRect,
            scale_factor: 1,
            name_real: "phasor_real".to_string(),
            name_imag: "phasor_imag".to_string(),
        }];

        let manager = AccumulatorManager::new_with_params(configs, phasor_configs, 10, 16, 5);

        // Duplicate the manager
        let duplicate = manager.duplicate();

        // Check that the duplicate has the same configuration
        assert_eq!(duplicate.configs.len(), 1);
        assert_eq!(duplicate.phasor_configs.len(), 1);
        assert_eq!(duplicate.schema.fields().len(), 3); // 1 regular + 2 phasor components

        // The schema should have fields in the correct order
        assert_eq!(duplicate.schema.field(0).name(), "value");
        assert_eq!(duplicate.schema.field(1).name(), "phasor_real");
        assert_eq!(duplicate.schema.field(2).name(), "phasor_imag");
    }
}
