// End-to-end test for the RTPA-core library.
//
// TEST #1
// Use the test data to parse a config frame.
// Use that config frame to create an accumulator manager and accumulators for each variable (including timestamp)
// Initialize the accumulator manager with the parsed config frame
//
// Read in the test data file and parse it up to manager batch size.
// Query and return a dataframe of the parsed data.
//
// Ensure that each column is correctly parsed. E.g. all values in a given column are the same.
// Values match the parsed data frame values expected.

use arrow::datatypes::DataType;
use std::fs;
use std::path::Path;

use rtpa_core::accumulator::manager::AccumulatorManager;
use rtpa_core::ieee_c37_118::create_configuration_frame;
use rtpa_core::utils::config_to_accumulators;

// Helper function to read test data files
fn read_hex_file(file_name: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let path = Path::new("tests/test_data").join(file_name);
    let content = fs::read_to_string(path)?;

    // Remove any whitespace and newlines
    let hex_string: String = content.chars().filter(|c| !c.is_whitespace()).collect();

    // Ensure we have an even number of hex characters
    if hex_string.len() % 2 != 0 {
        return Err("Invalid hex string: odd number of characters".into());
    }

    // Convert pairs of hex characters to bytes
    let mut result = Vec::with_capacity(hex_string.len() / 2);
    let mut i = 0;
    while i < hex_string.len() {
        let byte_str = &hex_string[i..i + 2];
        let byte = u8::from_str_radix(byte_str, 16)?;
        result.push(byte);
        i += 2;
    }

    Ok(result)
}

#[test]
fn test_ieee_c37_118_e2e() {
    // Step 1: Parse the config frame from test data
    let config_buffer = read_hex_file("config_message.bin").expect("Failed to read config file");
    let config_frame =
        create_configuration_frame(&config_buffer).expect("Failed to parse configuration frame");

    // Get the channel map from the config frame
    let channel_map = config_frame.get_channel_map();

    // Print the channel map for debugging
    println!("Channel Map:");
    for (name, info) in &channel_map {
        println!(
            "{}: {:?}, offset={}, size={}",
            name, info.data_type, info.offset, info.size
        );
    }

    // Step 2: Create accumulators for each variable
    let accumulator_configs = config_to_accumulators(&*config_frame);

    // Step 3: Create the accumulator manager
    let num_threads = 2; // Use 2 threads for the test
    let max_batches = 5; // Keep up to 5 batches in memory
    let data_frame_size = config_frame.calc_data_frame_size();
    let batch_size = 10; // Process 10 frames in a batch for testing

    println!(
        "Creating AccumulatorManager with buffer_size={}",
        data_frame_size
    );

    let accumulator_manager = AccumulatorManager::new_with_params(
        accumulator_configs,
        num_threads,
        max_batches,
        data_frame_size,
        batch_size,
    );

    // Step 4: Process test data frames
    let data_buffer = read_hex_file("data_message.bin").expect("Failed to read data file");

    // Process the same data frame multiple times to create a batch
    for i in 0..batch_size {
        println!("Processing data frame {}", i + 1);

        // Use closure to write the data to the buffer
        let result = accumulator_manager.process_buffer(|buffer| {
            buffer.clear();
            buffer.extend_from_slice(&data_buffer);
            data_buffer.len()
        });

        assert!(
            result.is_ok(),
            "Failed to process buffer: {:?}",
            result.err()
        );
    }

    // Step 5: Query the dataframe
    let all_columns: Vec<usize> = (0..accumulator_manager.schema().fields().len()).collect();
    let dataframe = accumulator_manager
        .get_dataframe(&all_columns, 10) // Get last 10 seconds of data
        .expect("Failed to get dataframe");

    // Step 6: Verify the data
    println!("Dataframe schema: {:?}", dataframe.schema());
    println!("Dataframe row count: {}", dataframe.num_rows());

    // Every row should have the same values since we processed the same data frame multiple times
    assert_eq!(
        dataframe.num_rows(),
        batch_size,
        "Unexpected number of rows"
    );

    // Check the schema matches our expectations
    let schema = dataframe.schema();
    assert!(schema.fields().len() > 0, "Schema has no fields");

    // Check each column to verify all values are the same
    for col_idx in 0..dataframe.columns().len() {
        let column_name = schema.field(col_idx).name();
        println!("Checking column: {}", column_name);

        let column = dataframe.column(col_idx);
        if column.len() <= 1 {
            continue; // Skip columns with only one value
        }

        // Check if all values in this column are the same
        // This is a bit complex since we need to handle different array types
        let all_same = match column.data_type() {
            DataType::Int32 => {
                let array = column
                    .as_any()
                    .downcast_ref::<arrow::array::Int32Array>()
                    .unwrap();
                let first_value = array.value(0);
                println!("Column {} first value: {}", column_name, first_value);
                let all_same = (1..array.len()).all(|i| array.value(i) == first_value);
                all_same
            }
            DataType::Float32 => {
                let array = column
                    .as_any()
                    .downcast_ref::<arrow::array::Float32Array>()
                    .unwrap();
                let first_value = array.value(0);
                println!("Column {} first value: {}", column_name, first_value);
                let all_same =
                    (1..array.len()).all(|i| (array.value(i) - first_value).abs() < 0.01);
                all_same
            }
            DataType::UInt16 => {
                let array = column
                    .as_any()
                    .downcast_ref::<arrow::array::UInt16Array>()
                    .unwrap();
                let first_value = array.value(0);
                println!("Column {} first value: {}", column_name, first_value);
                let all_same = (1..array.len()).all(|i| array.value(i) == first_value);
                all_same
            }
            DataType::Int16 => {
                let array = column
                    .as_any()
                    .downcast_ref::<arrow::array::Int16Array>()
                    .unwrap();
                let first_value = array.value(0);
                println!("Column {} first value: {}", column_name, first_value);
                let all_same = (1..array.len()).all(|i| array.value(i) == first_value);
                all_same
            }
            DataType::Int64 => {
                let array = column
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap();
                let first_value = array.value(0);
                println!("Column {} first value: {}", column_name, first_value);
                let all_same = (1..array.len()).all(|i| array.value(i) == first_value);
                all_same
            }
            _ => panic!("Unexpected column data type: {:?}", column.data_type()),
        };

        assert!(
            all_same,
            "Values in column {} are not all the same",
            column_name
        );
    }

    // Step 7: Check specific values
    // Verify the frequency column data (Station A_7734_FREQ should be 2500)
    let freq_column_name = "Station A_7734_FREQ";
    let freq_index = schema
        .fields()
        .iter()
        .position(|f| f.name() == freq_column_name)
        .expect("Frequency column not found");

    // Verify the frequency column data
    if let Some(array) = dataframe
        .column(freq_index)
        .as_any()
        .downcast_ref::<arrow::array::Int16Array>()
    {
        for i in 0..array.len() {
            assert_eq!(
                array.value(i),
                2500,
                "Unexpected frequency value at index {}",
                i
            );
        }
    } else if let Some(array) = dataframe
        .column(freq_index)
        .as_any()
        .downcast_ref::<arrow::array::Float32Array>()
    {
        for i in 0..array.len() {
            assert!(
                (array.value(i) - 2500.0).abs() < 0.01,
                "Unexpected frequency value at index {}",
                i
            );
        }
    } else {
        panic!("Frequency column has unexpected type");
    }

    // Clean up resources
    accumulator_manager.shutdown();
}
