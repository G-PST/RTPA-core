#[cfg(test)]
mod tests {
    use rtpa_cli::pdc_server::{run_mock_server, Protocol, ServerConfig};
    use rtpa_core::ieee_c37_118::common::Version;
    use rtpa_core::pdc_buffer::PDCBuffer;

    use std::thread;
    use std::time::Duration;
    use tokio::runtime::Runtime;
    use tokio::sync::oneshot;

    #[test]
    fn test_pdc_buffer_stream() {
        // Create a tokio runtime for the test
        let rt = Runtime::new().unwrap();

        // Configure the mock server
        let server_config = ServerConfig::new(
            "127.0.0.1".to_string(),
            4712,
            Protocol::TCP,
            120.0, // 10 Hz data rate
            None,
            "2011",
            false,
        )
        .expect("Failed to create server config");

        // Create a oneshot channel for shutdown signaling
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        // Spawn the mock server in a background thread
        let server_handle = thread::spawn(move || {
            rt.block_on(async {
                // Run the server until shutdown signal is received
                tokio::select! {
                    result = run_mock_server(server_config) => {
                        if let Err(e) = result {
                            eprintln!("Mock server error: {}", e);
                        }
                    }
                    _ = shutdown_rx => {
                        println!("Mock server shutting down");
                    }
                }
            });
        });

        // Give the server a moment to start
        thread::sleep(Duration::from_millis(500));

        // Initialize the PDCBuffer
        let mut pdc_buffer = PDCBuffer::new(
            "127.0.0.1".to_string(),
            4712,
            1, // ID code
            Some(Version::V2011),
            None,
            None,
            None,
        );

        // Start the stream
        println!("Starting Stream");
        pdc_buffer.start_stream();

        // Wait for a few seconds to allow data to flow
        thread::sleep(Duration::from_secs(3));
        // Get data from the buffer and check that it's not empty
        println!("Getting data from buffer");
        let data_result = pdc_buffer.get_data(None, Some(3));

        // Check that we got data successfully
        assert!(
            data_result.is_ok(),
            "Failed to get data from PDC buffer: {:?}",
            data_result.err()
        );

        let data = data_result.unwrap();

        // Print the row count for verification
        println!("Retrieved data with {} rows", data.num_rows());

        // Verify that we got at least some data
        assert!(data.num_rows() > 0, "Retrieved data has 0 rows");

        // Print schema and column info
        println!("Schema: {:?}", data.schema());
        for i in 0..data.num_columns() {
            println!(
                "Column {}: {} with {} rows",
                i,
                data.schema().field(i).name(),
                data.column(i).len()
            );
        }
        // Stop the stream
        println!("Stopping Stream");
        pdc_buffer.stop_stream();

        println!("Shutting down mock server");
        // Send shutdown signal to the server
        let _ = shutdown_tx.send(());

        // Wait for the server thread to finish
        server_handle.join().expect("Failed to join server thread");

        // Assert any conditions if needed (e.g., check if data was received)
        // For now, we just ensure the stream starts and stops without panicking
    }

    #[test]
    fn test_pdc_buffer_with_multiple_pmus() {
        // Create a tokio runtime for the test
        let rt = Runtime::new().unwrap();

        // Configure the mock server with multiple PMUs
        let server_config = ServerConfig::new(
            "127.0.0.1".to_string(),
            4713,
            Protocol::TCP,
            30.0,    // 30 Hz data rate
            Some(3), // 3 PMUs
            "2011",  // IEEE C37.118-2011
            false,   // rectangular coordinates
        )
        .expect("Failed to create server config");

        // Create a oneshot channel for shutdown signaling
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        // Spawn the mock server in a background thread
        let server_handle = thread::spawn(move || {
            rt.block_on(async {
                // Run the server until shutdown signal is received
                tokio::select! {
                    result = run_mock_server(server_config) => {
                        if let Err(e) = result {
                            eprintln!("Mock server error: {}", e);
                        }
                    }
                    _ = shutdown_rx => {
                        println!("Mock server shutting down");
                    }
                }
            });
        });

        // Give the server a moment to start
        thread::sleep(Duration::from_millis(500));

        // Initialize the PDCBuffer
        let mut pdc_buffer = PDCBuffer::new(
            "127.0.0.1".to_string(),
            4713,
            123, // ID code that matches the mock server
            Some(Version::V2011),
            None,
            None,
            None,
        );

        // Start the stream
        println!("Starting Stream");
        pdc_buffer.start_stream();

        // Wait to collect some data
        thread::sleep(Duration::from_secs(3));

        // Test getting configuration as JSON
        let config_json = pdc_buffer
            .config_to_json()
            .expect("Failed to get configuration JSON");
        println!("Configuration JSON length: {} bytes", config_json.len());
        assert!(
            !config_json.is_empty(),
            "Configuration JSON should not be empty"
        );

        // Verify the config contains 3 PMUs
        assert!(
            config_json.contains("\"num_pmu\":3"),
            "Configuration should contain 3 PMUs"
        );

        // Retrieve data
        let data_result = pdc_buffer.get_data(None, Some(3)).unwrap();
        println!(
            "Retrieved data with {} rows and {} columns",
            data_result.num_rows(),
            data_result.num_columns()
        );

        // Print schema to see the columns generated from the multiple PMUs
        println!("Schema: {:?}", data_result.schema());

        // Find the first two channel names for the first PMU to use in our test
        let schema = data_result.schema();
        let mut pmu0_channels = Vec::new();

        // Find channel names that contain "STATION00" and "PH_00" for the first PMU
        for i in 0..schema.fields().len() {
            let field_name = schema.field(i).name();
            if field_name.contains("STATION00") && field_name.contains("PH_00") {
                pmu0_channels.push(field_name.as_str());
                if pmu0_channels.len() >= 2 {
                    break;
                }
            }
        }

        println!("Found PMU0 channels: {:?}", pmu0_channels);

        // Now get data for just these channels
        if pmu0_channels.len() >= 2 {
            let first_pmu_data = pdc_buffer.get_data(Some(pmu0_channels), Some(3)).unwrap();
            println!(
                "First PMU data has {} rows and {} columns",
                first_pmu_data.num_rows(),
                first_pmu_data.num_columns()
            );

            assert!(
                first_pmu_data.num_columns() == 2,
                "Should have retrieved 2 columns from the first PMU"
            );
        } else {
            println!("Couldn't find enough PMU0 channels, skipping specific channel test");
        }

        // Get the latest buffer (raw frame)
        let buffer = pdc_buffer
            .get_latest_buffer()
            .expect("Failed to get latest buffer");
        println!("Retrieved raw buffer with {} bytes", buffer.len());
        assert!(!buffer.is_empty(), "Buffer should not be empty");

        // Stop the stream
        println!("Stopping Stream");
        pdc_buffer.stop_stream();

        println!("Shutting down mock server");
        // Send shutdown signal to the server
        let _ = shutdown_tx.send(());

        // Wait for the server thread to finish
        server_handle.join().expect("Failed to join server thread");
    }
}
