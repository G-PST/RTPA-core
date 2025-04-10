#[cfg(test)]
mod tests {
    use rtpa_core::ieee_c37_118::VersionStandard;
    use rtpa_core::pdc_buffer::PDCBuffer;
    use rtpa_core::pdc_server::{run_mock_server, Protocol, ServerConfig};

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
            Some(VersionStandard::Ieee2011),
        );

        // Start the stream
        println!("Starting Stream");
        pdc_buffer.start_stream();

        // Wait for a few seconds to allow data to flow
        thread::sleep(Duration::from_secs(3));

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
    // New test for Python bindings
}
