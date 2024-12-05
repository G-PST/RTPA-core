use chrono::{Duration, Utc};
use pmu::pdc_buffer_server;
use pmu::pdc_server::{run_mock_server, Protocol, ServerConfig};
use std::time::Duration as StdDuration;
use tokio::time::sleep;

#[tokio::test]
async fn test_filtered_queries() {
    // Create server configuration
    let server_config = ServerConfig::new(
        "127.0.0.1".to_string(),
        8123, // Port for mock PDC
        Protocol::TCP,
        30.0, // Data rate in Hz
    )
    .unwrap();

    // Start mock PDC in background
    tokio::spawn(async {
        run_mock_server(server_config).await.unwrap();
    });

    // Give mock PDC time to start
    sleep(StdDuration::from_secs(1)).await;

    // Start PDC buffer server in background
    tokio::spawn(async {
        pdc_buffer_server::run().await.unwrap();
    });

    // Give server time to start and collect some data
    sleep(StdDuration::from_secs(5)).await;

    // Create HTTP client
    let client = reqwest::Client::new();
    let base_url = "http://localhost:8080";

    // Test 1: Time-based filtering
    let now = Utc::now().timestamp_micros();
    let five_seconds_ago = (Utc::now() - Duration::seconds(5)).timestamp_micros();
    sleep(StdDuration::from_secs(5)).await;

    let response0 = client
        .get(format!("{}/data", base_url))
        .send()
        .await
        .unwrap();

    assert_eq!(response0.status(), 200);
    assert_eq!(response0.headers()["content-type"], "application/arrow-ipc");

    let response1 = client
        .get(format!(
            "{}/data?start_time={}&end_time={}",
            base_url, five_seconds_ago, now
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(response1.status(), 200);
    assert_eq!(response1.headers()["content-type"], "application/arrow-ipc");

    // Test 2: Column filtering
    let column_selection = serde_json::json!({
        "columns": ["Station A_7734_VA", "Station A_7734_VB"]
    });

    let response2 = client
        .post(format!("{}/data", base_url))
        .json(&column_selection)
        .send()
        .await
        .unwrap();

    assert_eq!(response2.status(), 200);
    assert_eq!(response2.headers()["content-type"], "application/arrow-ipc");

    // Test 3: Combined filtering
    let response3 = client
        .post(format!(
            "{}/data?start_time={}&end_time={}",
            base_url, five_seconds_ago, now
        ))
        .json(&column_selection)
        .send()
        .await
        .unwrap();

    assert_eq!(response3.status(), 200);
    assert_eq!(response3.headers()["content-type"], "application/arrow-ipc");

    use arrow::ipc::reader::FileReader;
    use std::io::Cursor;

    // Helper function to count rows and columns in Arrow IPC response
    async fn verify_arrow_response(response: reqwest::Response) -> (usize, usize) {
        let body = response.bytes().await.unwrap();
        let cursor = Cursor::new(body);
        let reader = FileReader::try_new(cursor, None).unwrap();
        let batch = reader.into_iter().next().unwrap().unwrap();
        (batch.num_rows(), batch.num_columns())
    }

    // Verify Test 1 (time filtering)
    let (rows1, _cols1) = verify_arrow_response(response1).await;
    assert!(rows1 > 0, "Should have some rows");

    // Verify Test 2 (column filtering)
    let (rows2, cols2) = verify_arrow_response(response2).await;
    assert!(rows2 > 0, "Should have some rows");
    assert_eq!(cols2, 5); // timestamp + 2*2 selected columns
                          // We get twice as many columns if they are phasor

    // Verify Test 3 (combined filtering)
    let (rows3, cols3) = verify_arrow_response(response3).await;
    assert!(rows3 > 0, "Should have some rows");
    assert_eq!(cols3, 5); // timestamp + 2*2 selected columns
    assert!(
        rows3 <= rows2,
        "Should have fewer or equal rows than unfiltered"
    );
}
