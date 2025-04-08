// This file contains tests for arrow alignment in the sparse-accumulator crate.
//
// # TEST 1
// This should create a single static buffer and call the process_buffer function multiple times. (similar to main.rs)
// The resulting dataframe should be generated and each column should have the same value repeated multiple times.
// We should also test that we get the number of correct rows.
//
// # TEST 2
// Another test should test that we are able to create a record batch from a partially filled buffer and concatenate it with previous record batches that are filled to the batch size.
// For this test, we should run the process buffer function 1.5 times the batch size.
//
// # TEST 3
// We should also test the we are able to correctly create data of each data type, (u16, f32, i32)
// To do this, we should create f32, i32 and u16 values, convert them to u8 values and combine into a single buffer to process_buffer a few times.
//
// # TEST 4
// In this test, we should create a random dataframe of with columns of various types.
// Next we should convert each row into a buffer of bytes to be processed by the process_buffer function.
// We should create an arrow dataframe from the manager and ensure that it matches the random arrow dataframe from the beginning.
//
// # TEST 5 (Updates to source code required)
// There should be a test for SparseAccumulators that are out of bounds of the buffer.
// TODO, we need to update the code such that accumulators that are out of bounds throw a warning visible to the end user
// and removed from the list of that particular threads accumulators.
//
// # TEST 6
// Test that old record batches are properly evicted. Use a small number of batches to keep in memory and ensure the length of
// the vec holding record batches doesn't increase.
use arrow::array::{Array, Float32Array, Int32Array, UInt16Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rand::random;
use std::sync::Arc;

use sparse_accumulator::accumulator::manager::AccumulatorManager;

// Create a simplified builder for the tests
pub struct RecordBatchManagerBuilder {
    schema: Option<Arc<Schema>>,
    batch_size: usize,
    max_batches: usize,
    buffer_size: usize,
}

impl RecordBatchManagerBuilder {
    pub fn new() -> Self {
        Self {
            schema: None,
            batch_size: 120,  // Default from the manager
            max_batches: 10,  // Default value
            buffer_size: 512, // Default for tests
        }
    }

    pub fn with_schema(mut self, schema: Arc<Schema>) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn with_max_batches(mut self, max_batches: usize) -> Self {
        self.max_batches = max_batches;
        self
    }

    pub fn with_buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    pub fn build(self) -> sparse_accumulator::accumulator::manager::AccumulatorManager {
        let schema = self.schema.expect("Schema must be provided");

        let configs = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                sparse_accumulator::accumulator::manager::AccumulatorConfig {
                    var_loc: (i * 4) as u16, // Put each field 4 bytes apart
                    var_len: match field.data_type() {
                        DataType::Float32 | DataType::Int32 => 4,
                        DataType::UInt16 => 2,
                        _ => panic!("Unsupported data type: {:?}", field.data_type()),
                    },
                    var_type: field.data_type().clone(),
                    name: field.name().clone(),
                }
            })
            .collect::<Vec<_>>();

        sparse_accumulator::accumulator::manager::AccumulatorManager::new_with_params(
            configs,
            2, // Use 2 threads for testing
            self.max_batches,
            self.buffer_size,
            self.batch_size,
        )
    }
}

// Helper function to process a buffer in tests
fn process_buffer(manager: &AccumulatorManager, buffer: &[u8]) -> Result<(), String> {
    let buffer_copy = buffer.to_vec(); // Make a copy to avoid lifetime issues

    manager.process_buffer(|dst| {
        let len = std::cmp::min(dst.len(), buffer_copy.len());
        dst[..len].copy_from_slice(&buffer_copy[..len]);
        len
    })
}

#[test]
fn test_process_buffer_multiple_times() {
    // TEST 1: Create static buffer and process multiple times

    // Create a schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("f32_col", DataType::Float32, false),
        Field::new("i32_col", DataType::Int32, false),
    ]));

    // Size of our buffer
    let buffer_size = 8; // 4 bytes for f32 + 4 bytes for i32

    // Create manager with batch size of 10
    let manager = RecordBatchManagerBuilder::new()
        .with_schema(schema.clone())
        .with_batch_size(10)
        .with_buffer_size(buffer_size)
        .build();

    // Create static buffer with f32=1.5 and i32=42
    let mut buffer = vec![0u8; buffer_size];
    buffer[0..4].copy_from_slice(&1.5f32.to_le_bytes());
    buffer[4..8].copy_from_slice(&42i32.to_le_bytes());

    // Process buffer multiple times (15 times = 15 rows)
    for _ in 0..15 {
        process_buffer(&manager, &buffer).unwrap();
    }

    // Flush any pending data
    manager.flush_pending_batch();

    // Get resulting dataframe
    let df = manager.get_dataframe(&[0, 1], 60).unwrap();

    // Verify number of rows
    assert_eq!(df.num_rows(), 15, "Expected 15 rows in the result");

    // Verify column values
    let f32_col = df
        .column(0)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    let i32_col = df.column(1).as_any().downcast_ref::<Int32Array>().unwrap();

    for i in 0..15 {
        assert_eq!(
            f32_col.value(i),
            1.5,
            "Unexpected value in f32 column at row {}",
            i
        );
        assert_eq!(
            i32_col.value(i),
            42,
            "Unexpected value in i32 column at row {}",
            i
        );
    }
}

#[test]
fn test_partial_batch() {
    // TEST 2: Test partial batch creation

    // Create a schema
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Float32,
        false,
    )]));

    // Size of our buffer
    let buffer_size = 4; // 4 bytes for f32

    // Create manager with batch size of 10
    let manager = RecordBatchManagerBuilder::new()
        .with_schema(schema.clone())
        .with_batch_size(10)
        .with_buffer_size(buffer_size)
        .build();

    // Create buffer with a single f32 value
    let mut buffer = vec![0u8; buffer_size];
    buffer[0..4].copy_from_slice(&3.14f32.to_le_bytes());

    // Process buffer 15 times (1.5 * batch_size)
    for _i in 0..15 {
        process_buffer(&manager, &buffer).unwrap();
    }

    // Flush any pending data
    manager.flush_pending_batch();

    // Get resulting dataframe
    let df = manager.get_dataframe(&[0], 60).unwrap();

    // Verify number of rows (should be 15)
    assert_eq!(df.num_rows(), 15, "Expected 15 rows in the result");

    // Verify values
    let value_col = df
        .column(0)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    for i in 0..15 {
        assert_eq!(value_col.value(i), 3.14, "Unexpected value at row {}", i);
    }
}

#[test]
fn test_multiple_data_types() {
    // TEST 3: Test different data types

    // Create a schema with different data types
    let schema = Arc::new(Schema::new(vec![
        Field::new("f32_col", DataType::Float32, false),
        Field::new("i32_col", DataType::Int32, false),
        Field::new("u16_col", DataType::UInt16, false),
    ]));

    // Size of our buffer
    let buffer_size = 12; // 4 bytes for f32 + 4 bytes for i32 + 2 bytes for u16 + 2 bytes padding

    // Create manager
    let manager = RecordBatchManagerBuilder::new()
        .with_schema(schema.clone())
        .with_batch_size(5)
        .with_buffer_size(buffer_size)
        .build();

    // Create buffer with all three types
    let mut buffer = vec![0u8; buffer_size];
    buffer[0..4].copy_from_slice(&2.5f32.to_le_bytes());
    buffer[4..8].copy_from_slice(&(-100i32).to_le_bytes());
    buffer[8..10].copy_from_slice(&42u16.to_le_bytes());

    // Process buffer multiple times
    for _ in 0..7 {
        process_buffer(&manager, &buffer).unwrap();
    }

    // Flush any pending data
    manager.flush_pending_batch();

    // Get resulting dataframe
    let df = manager.get_dataframe(&[0, 1, 2], 60).unwrap();

    // Verify number of rows
    assert_eq!(df.num_rows(), 7, "Expected 7 rows in the result");

    // Verify column types and values
    let f32_col = df
        .column(0)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    let i32_col = df.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
    let u16_col = df.column(2).as_any().downcast_ref::<UInt16Array>().unwrap();

    for i in 0..7 {
        assert_eq!(
            f32_col.value(i),
            2.5,
            "Unexpected value in f32 column at row {}",
            i
        );
        assert_eq!(
            i32_col.value(i),
            -100,
            "Unexpected value in i32 column at row {}",
            i
        );
        assert_eq!(
            u16_col.value(i),
            42,
            "Unexpected value in u16 column at row {}",
            i
        );
    }
}

#[test]
fn test_random_dataframe_to_buffer_and_back() {
    // TEST 4: Create random dataframe, convert to buffer, process, and verify

    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("f32_col", DataType::Float32, false),
        Field::new("i32_col", DataType::Int32, false),
        Field::new("u16_col", DataType::UInt16, false),
    ]));

    // Size of our buffer
    let buffer_size = 12; // 4 bytes for f32 + 4 bytes for i32 + 2 bytes for u16 + 2 bytes padding

    // Generate random data
    let num_rows = 20;
    let mut f32_values = Vec::with_capacity(num_rows);
    let mut i32_values = Vec::with_capacity(num_rows);
    let mut u16_values = Vec::with_capacity(num_rows);

    for _ in 0..num_rows {
        f32_values.push(random::<f32>());
        i32_values.push(random::<i32>());
        u16_values.push(random::<u16>());
    }

    // Create original Arrow arrays
    let f32_array = Float32Array::from(f32_values.clone());
    let i32_array = Int32Array::from(i32_values.clone());
    let u16_array = UInt16Array::from(u16_values.clone());

    // Create original record batch
    let _original_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(f32_array),
            Arc::new(i32_array),
            Arc::new(u16_array),
        ],
    )
    .unwrap();

    // Create manager
    let manager = RecordBatchManagerBuilder::new()
        .with_schema(schema.clone())
        .with_batch_size(num_rows)
        .with_buffer_size(buffer_size)
        .build();

    // Convert each row to buffer and process
    for i in 0..num_rows {
        let mut buffer = vec![0u8; buffer_size];
        buffer[0..4].copy_from_slice(&f32_values[i].to_le_bytes());
        buffer[4..8].copy_from_slice(&i32_values[i].to_le_bytes());
        buffer[8..10].copy_from_slice(&u16_values[i].to_le_bytes());

        process_buffer(&manager, &buffer).unwrap();
    }

    // Flush any pending data
    manager.flush_pending_batch();

    // Get resulting dataframe
    let result_batch = manager.get_dataframe(&[0, 1, 2], 60).unwrap();

    // Verify number of rows
    assert_eq!(
        result_batch.num_rows(),
        num_rows,
        "Expected {} rows in the result",
        num_rows
    );

    // Verify column values
    let f32_col = result_batch
        .column(0)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    let i32_col = result_batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let u16_col = result_batch
        .column(2)
        .as_any()
        .downcast_ref::<UInt16Array>()
        .unwrap();

    for i in 0..num_rows {
        assert!(
            (f32_col.value(i) - f32_values[i]).abs() < f32::EPSILON,
            "Unexpected value in f32 column at row {}",
            i
        );
        assert_eq!(
            i32_col.value(i),
            i32_values[i],
            "Unexpected value in i32 column at row {}",
            i
        );
        assert_eq!(
            u16_col.value(i),
            u16_values[i],
            "Unexpected value in u16 column at row {}",
            i
        );
    }
}

#[test]
fn test_out_of_bounds_accumulators() {
    // TEST 5: Test for SparseAccumulators that are out of bounds of the buffer

    // We'll set up a small buffer size
    let buffer_size = 8;

    // Create manager manually with custom configs, including an out-of-bounds one
    let configs = vec![
        sparse_accumulator::accumulator::manager::AccumulatorConfig {
            var_loc: 0,
            var_len: 4,
            var_type: DataType::Float32,
            name: "in_bounds".to_string(),
        },
        sparse_accumulator::accumulator::manager::AccumulatorConfig {
            var_loc: 1000, // This is intentionally beyond buffer size
            var_len: 4,
            var_type: DataType::Int32,
            name: "out_of_bounds".to_string(),
        },
    ];

    // Create the manager with our custom buffer size
    let manager = sparse_accumulator::accumulator::manager::AccumulatorManager::new_with_params(
        configs,
        1,  // Use 1 thread to simplify the test
        10, // max batches
        buffer_size,
        10, // batch size
    );

    // Create a small buffer
    let mut buffer = vec![0u8; buffer_size];
    buffer[0..4].copy_from_slice(&3.14f32.to_le_bytes());

    // Process buffer
    process_buffer(&manager, &buffer).unwrap();

    // Flush any pending data
    manager.flush_pending_batch();

    // Get resulting dataframe - should only have the in-bounds column
    let df = manager.get_dataframe(&[0], 60).unwrap();

    // Verify the in-bounds column has data
    assert!(df.num_rows() > 0, "Should have some rows in the result");

    // Check the in-bounds column value
    let in_bounds_col = df
        .column(0)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    for i in 0..df.num_rows() {
        assert_eq!(
            in_bounds_col.value(i),
            3.14,
            "Unexpected value in in_bounds column at row {}",
            i
        );
    }
}

#[test]
fn test_batch_eviction() {
    // TEST 6: Test that old record batches are properly evicted

    // Create schema
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Float32,
        false,
    )]));

    // Size of our buffer
    let buffer_size = 4; // 4 bytes for f32

    // Create manager with batch size of 5 and max batches of 2
    // This means we can store at most 10 rows (2 batches * 5 rows) before eviction
    let manager = RecordBatchManagerBuilder::new()
        .with_schema(schema.clone())
        .with_batch_size(5)
        .with_max_batches(2)
        .with_buffer_size(buffer_size)
        .build();

    // Create buffer with a single f32 value
    let mut buffer = vec![0u8; buffer_size];
    buffer[0..4].copy_from_slice(&3.14f32.to_le_bytes());

    // Process buffer enough times to fill multiple batches
    // We'll process 25 times, which should create 5 batches, but only keep the last 2
    for _ in 0..25 {
        process_buffer(&manager, &buffer).unwrap();
    }

    // Flush any pending data
    manager.flush_pending_batch();

    // Get all the data
    let all_data = manager.get_dataframe(&[0], 60).unwrap();

    // We should have at most 2 batches * 5 rows = 10 rows
    assert!(
        all_data.num_rows() <= 10,
        "Should have at most 10 rows after eviction"
    );

    // Verify the values
    let value_col = all_data
        .column(0)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    for i in 0..all_data.num_rows() {
        assert_eq!(value_col.value(i), 3.14, "Unexpected value at row {}", i);
    }
}
