use super::sparse::{
    Accumulate, Accumulator, C37118TimestampAccumulator, F32Accumulator, I32Accumulator,
    U16Accumulator,
};
use arrow::array::{ArrayRef, Float32Array, Int32Array, UInt16Array};
use arrow::buffer::{Buffer, MutableBuffer};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use core_affinity;
use rayon::prelude::*;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const MAX_BUFFER_SIZE: usize = 55 * 1024;
const BATCH_SIZE: usize = 120; // Rows per batch (e.g., 1s at 60 Hz)
                               // Should we do 128 for alignment?

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
    input_buffer: Arc<RwLock<Vec<u8>>>,
    written_bytes: Arc<RwLock<usize>>,
    shutdown: Arc<AtomicBool>,
    work_senders: Vec<Sender<()>>,     // Signal work to threads
    completion_receiver: Receiver<()>, // Receive completion signals            // Ensure threads are ready
    threads: Vec<JoinHandle<()>>,
    batch_index: Arc<RwLock<usize>>,
    max_batches: usize,
    num_threads: usize, // Added to track expected thread count
    buffer_size: usize,
    batch_size: usize,
}

impl AccumulatorManager {
    pub fn new(configs: Vec<AccumulatorConfig>, num_threads: usize, max_batches: usize) -> Self {
        Self::new_with_params(
            configs,
            num_threads,
            max_batches,
            MAX_BUFFER_SIZE,
            BATCH_SIZE,
        )
    }

    pub fn new_with_params(
        configs: Vec<AccumulatorConfig>,
        num_threads: usize,
        max_batches: usize,
        buffer_size: usize,
        batch_size: usize,
    ) -> Self {
        // First, check that all accumulators have valid location and length
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
        let output_buffers: Vec<Arc<Mutex<MutableBuffer>>> = valid_configs
            .iter()
            .map(|config| {
                let capacity = match config.var_type {
                    DataType::Float32 => batch_size * std::mem::size_of::<f32>(),
                    DataType::Int32 => batch_size * std::mem::size_of::<i32>(),
                    DataType::UInt16 => batch_size * std::mem::size_of::<u16>(),
                    DataType::Int64 => batch_size * std::mem::size_of::<i64>(),
                    DataType::Timestamp(_, _) => batch_size * std::mem::size_of::<i64>(),
                    _ => panic!("Unsupported data type: {:?}", config.var_type),
                };
                Arc::new(Mutex::new(MutableBuffer::new(capacity)))
            })
            .collect();

        // Create the shared resources
        let input_buffer: Arc<RwLock<Vec<u8>>> =
            Arc::new(RwLock::new(Vec::with_capacity(buffer_size)));
        let written_bytes = Arc::new(RwLock::new(0));
        let batch_index = Arc::new(RwLock::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));
        let records: Arc<Mutex<VecDeque<(u64, RecordBatch)>>> =
            Arc::new(Mutex::new(VecDeque::with_capacity(max_batches)));

        let (completion_sender, completion_receiver) = channel();
        let mut work_senders = Vec::new();
        let mut threads = Vec::new();

        let core_ids = core_affinity::get_core_ids().unwrap();
        assert!(
            num_threads <= core_ids.len(),
            "Too many threads for available cores"
        );

        let accumulators_per_thread = (valid_configs.len() + num_threads - 1) / num_threads;

        for i in 0..num_threads {
            let core_id = core_ids[i];
            let start_idx = i * accumulators_per_thread;
            let end_idx = std::cmp::min(start_idx + accumulators_per_thread, valid_configs.len());
            if start_idx >= valid_configs.len() {
                break;
            }

            let thread_configs = valid_configs[start_idx..end_idx].to_vec();
            let thread_buffers = output_buffers[start_idx..end_idx].to_vec();

            // Clone the shared resources for this thread
            let input_buffer_clone = input_buffer.clone();
            let written_bytes_clone = written_bytes.clone();
            let _batch_index_clone = batch_index.clone();
            let shutdown_clone = shutdown.clone();
            let completion_sender = completion_sender.clone();
            let (work_sender, work_receiver) = channel();
            let _thread_batch_size = batch_size;

            let handle = thread::spawn(move || {
                core_affinity::set_for_current(core_id);
                let accumulators: Vec<Accumulator> = thread_configs
                    .iter()
                    .map(|config| match (config.var_type.clone(), config.var_len) {
                        (DataType::UInt16, 2) => Accumulator::U16(U16Accumulator {
                            var_loc: config.var_loc,
                        }),
                        (DataType::Int32, 4) => Accumulator::I32(I32Accumulator {
                            var_loc: config.var_loc,
                        }),
                        (DataType::Float32, 4) => Accumulator::F32(F32Accumulator {
                            var_loc: config.var_loc,
                        }),
                        (DataType::Timestamp(_, _), 8) => {
                            Accumulator::Timestamp(C37118TimestampAccumulator {
                                var_loc: config.var_loc,
                            })
                        }
                        (var_type, var_len) => panic!(
                            "Unsupported (type, len) combination: {:?}, {}",
                            var_type, var_len
                        ),
                    })
                    .collect();

                while !shutdown_clone.load(Ordering::SeqCst) {
                    match work_receiver.recv() {
                        Ok(()) => {
                            let bytes = *written_bytes_clone.read().unwrap();
                            if bytes > 0 {
                                let input_data =
                                    input_buffer_clone.read().unwrap()[..bytes].to_vec();
                                for (acc, buffer) in accumulators.iter().zip(thread_buffers.iter())
                                {
                                    let mut buffer_guard = buffer.lock().unwrap();
                                    acc.accumulate(&input_data, &mut buffer_guard);
                                }
                            }

                            completion_sender.send(()).unwrap();
                        }
                        Err(_) => break, // Channel closed, shutdown
                    }
                }
            });

            work_senders.push(work_sender);
            threads.push(handle);
        }

        AccumulatorManager {
            output_buffers,
            configs: valid_configs,
            schema,
            records,
            input_buffer,
            written_bytes,
            shutdown,
            work_senders,
            completion_receiver,
            threads,
            batch_index,
            max_batches,
            num_threads,
            buffer_size,
            batch_size,
        }
    }

    // Convenience constructor with simple tuple inputs
    pub fn from_simple_configs(
        configs: Vec<(u16, u8, DataType, String)>,
        num_threads: usize,
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

        Self::new(acc_configs, num_threads, max_batches)
    }

    pub fn from_simple_configs_with_params(
        configs: Vec<(u16, u8, DataType, String)>,
        num_threads: usize,
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

        Self::new_with_params(
            acc_configs,
            num_threads,
            max_batches,
            buffer_size,
            batch_size,
        )
    }

    pub fn process_buffer<F>(&self, writer: F) -> Result<(), String>
    where
        F: FnOnce(&mut Vec<u8>) -> usize,
    {
        if self.shutdown.load(Ordering::SeqCst) {
            return Err("Manager is shutting down".to_string());
        }

        let written = {
            let mut input_guard = self.input_buffer.write().unwrap();
            input_guard.clear();
            input_guard.resize(self.buffer_size, 0);
            let bytes = writer(&mut input_guard);
            if bytes > self.buffer_size {
                return Err("Written data exceeds max buffer size".to_string());
            }
            bytes
        };

        if written == 0 {
            return Err("No data written to buffer".to_string());
        }

        // Check if the input buffer is filled to the expected length
        if written != self.buffer_size {
            println!(
                "WARNING: Input buffer not filled to expected size. Expected {}, got {}",
                self.buffer_size, written
            );
            return Err("Input buffer must be filled to the full buffer size".to_string());
        }

        // Update written bytes atomically
        *self.written_bytes.write().unwrap() = written;

        // Signal all worker threads
        for sender in &self.work_senders {
            match sender.send(()) {
                Ok(_) => {}
                Err(e) => return Err(format!("Failed to signal thread: {}", e)),
            }
        }

        // Use a timeout and counter to detect partial completions
        let mut completed_threads = 0;
        let timeout = Duration::from_secs(1); // 1 second timeout per thread

        while completed_threads < self.work_senders.len() {
            match self.completion_receiver.recv_timeout(timeout) {
                Ok(()) => {
                    completed_threads += 1;
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    println!(
                        "WARNING: Timeout waiting for thread completions, got {}/{}",
                        completed_threads,
                        self.work_senders.len()
                    );
                    // Continue anyway - this prevents permanent deadlock
                    break;
                }
                Err(e) => return Err(format!("Channel error: {}", e)),
            }
        }

        // Update batch index and check if we need to save
        let mut idx = self.batch_index.write().unwrap();
        *idx += 1;

        if *idx >= self.batch_size {
            self.save_batch();
            *idx = 0;
        }

        *self.written_bytes.write().unwrap() = 0;
        Ok(())
    }

    pub fn flush_pending_batch(&self) {
        let idx = *self.batch_index.read().unwrap();
        if idx > 0 {
            self.save_batch();
            *self.batch_index.write().unwrap() = 0;
        }
    }

    fn save_batch(&self) {
        let mut has_data = false;
        for buffer in self.output_buffers.iter() {
            let len = buffer.lock().unwrap().len();
            if len > 0 {
                has_data = true;
                break;
            }
        }

        if !has_data {
            println!("No data to save in any column");
            return;
        }

        // Copy MutableBuffers to immutable Buffers in parallel
        let arrays: Vec<ArrayRef> = self
            .output_buffers
            .par_iter()
            .zip(self.configs.par_iter())
            .map(|(buffer, config)| {
                let buffer_guard = buffer.lock().unwrap();
                let data_buffer = Buffer::from(&buffer_guard.as_slice()[..buffer_guard.len()]);

                match config.var_type {
                    DataType::Float32 => {
                        // Calculate how many complete f32 values we have
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
        let mut records = self.records.lock().unwrap();
        records.push_back((timestamp, batch));
        if records.len() > self.max_batches {
            records.pop_front();
        }

        // Reset MutableBuffers
        self.output_buffers.par_iter().for_each(|buffer| {
            let mut buffer_guard = buffer.lock().unwrap();
            buffer_guard.clear();
        });
    }

    pub fn shutdown(self) {
        self.shutdown.store(true, Ordering::SeqCst);
        // Dropping senders will close channels, causing threads to exit
        drop(self.work_senders);
        for handle in self.threads {
            if let Err(e) = handle.join() {
                eprintln!("Error joining thread: {:?}", e);
            }
        }
    }

    // Add accessor for the schema
    pub fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    pub fn get_dataframe(
        &self,
        columns: &[usize],
        window_secs: u64,
    ) -> Result<RecordBatch, String> {
        if self.shutdown.load(Ordering::SeqCst) {
            return Err("Manager is shutting down".to_string());
        }

        self.flush_pending_batch();
        // Force flush of current buffer, even if not full
        {
            let idx = *self.batch_index.read().unwrap();
            if idx > 0 {
                self.save_batch();
                *self.batch_index.write().unwrap() = 0;
            }
            if idx > 0 && *self.written_bytes.read().unwrap() > 0 {
                // Signal all threads to process the current buffer
                for sender in &self.work_senders {
                    sender
                        .send(())
                        .map_err(|e| format!("Failed to signal thread for flush: {}", e))?;
                }

                // Wait for all threads to complete
                for i in 0..self.num_threads {
                    match self
                        .completion_receiver
                        .recv_timeout(Duration::from_secs(5))
                    {
                        Ok(()) => {}
                        Err(_) => {
                            return Err(format!(
                                "Deadlock detected in get_dataframe flush: only {}/{} threads completed",
                                i, self.num_threads
                            ));
                        }
                    }
                }

                // Save the partial batch and reset state
                self.save_batch();
                *self.batch_index.write().unwrap() = 0;
                *self.written_bytes.write().unwrap() = 0;
            }
        }

        // Calculate time window
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let start_time = now.saturating_sub(window_secs * 1000);

        // Access stored records
        let records = self.records.lock().unwrap();
        if records.is_empty() {
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
                    dt => panic!("Unsupported data type: {:?}", dt),
                })
                .collect();

            return RecordBatch::try_new(sub_schema, empty_arrays)
                .map_err(|e| format!("Failed to create empty record batch: {}", e));
        }

        // Find the start index for the time window using binary search
        let start_idx = match records.binary_search_by(|(ts, _)| ts.cmp(&start_time)) {
            Ok(idx) => idx,
            Err(idx) => idx.saturating_sub(1).max(0),
        };

        // Collect batches within the window
        let batches: Vec<&RecordBatch> = records
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

        // First collect all column indices and schema data types
        let column_data: Vec<(usize, &DataType)> = columns
            .iter()
            .map(|&col_idx| (col_idx, self.schema.field(col_idx).data_type()))
            .collect();

        // Collect all the arrays sequentially first
        let arrays_by_column: Vec<Vec<&dyn arrow::array::Array>> = column_data
            .iter()
            .map(|&(col_idx, _)| {
                batches
                    .iter()
                    .map(|batch| batch.column(col_idx).as_ref())
                    .collect()
            })
            .collect();

        // Now we can safely use Rayon without sharing the AccumulatorManager
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
                        _ => panic!("Unsupported data type: {:?}", data_type),
                    }
                } else {
                    arrow::compute::concat(&arrays).unwrap()
                }
            })
            .collect();

        // Create and return the final RecordBatch
        RecordBatch::try_new(sub_schema, concatenated_arrays)
            .map_err(|e| format!("Failed to create record batch: {}", e))
    }
}
