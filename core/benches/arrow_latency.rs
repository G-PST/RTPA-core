// This file contains benchmarks to test the latency for creating arrow dataframes from the sparse accumulator manager.
//
// For this benchmark, we should measure the latency of creating arrow dataframes of different sizes. Number of rows and number of columns.
// This needs to follow a process similar to main.rs where we process a static buffer multiple times, and then create dataframes from the processed data.
//
// Ultimate test. A Buffer size of 55 * 1024 bytes, 120 seconds of data, and 2000 columns
//
mod utils;
use utils::{create_configs, create_test_buffer};

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use rand::Rng;
use rtpa_core::accumulator::manager::AccumulatorManager;
use std::time::Duration;

// Benchmark for getting dataframes of different sizes
fn bench_get_dataframe(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_dataframe");
    group.measurement_time(Duration::from_secs(15));

    let buffer_size = 1024;
    let test_buffer = create_test_buffer(buffer_size);

    // Test different combinations of rows and columns
    let test_cases = vec![
        // (rows to process, columns to fetch, total columns)
        (60, 2, 10),   // 1 second of data at 60Hz, few columns
        (120, 5, 10),  // 2 seconds of data, half the columns
        (300, 10, 10), // 5 seconds of data, all columns
        (600, 5, 20),  // 10 seconds of data, quarter of columns
    ];

    for (rows, cols_to_fetch, total_cols) in test_cases {
        group.bench_with_input(
            BenchmarkId::new(
                format!("rows_{}_fetch_{}_of_{}", rows, cols_to_fetch, total_cols),
                "",
            ),
            &(rows, cols_to_fetch, total_cols),
            |b, &(rows, cols_to_fetch, total_cols)| {
                let configs = create_configs(total_cols, buffer_size);
                let mut manager = AccumulatorManager::new(configs, vec![], 1000); // Allow more batches

                // Process the specified number of rows before starting the benchmark
                for _ in 0..rows {
                    manager.process_buffer(&test_buffer).unwrap();
                }

                // Create a vector of column indices to fetch
                let columns: Vec<usize> = (0..cols_to_fetch).collect();

                // Benchmark only the dataframe creation
                b.iter(|| {
                    black_box(manager._get_dataframe(&columns, 60).unwrap());
                });

                manager.shutdown();
            },
        );
    }

    group.finish();
}

// Benchmark different window sizes for time-based queries
fn bench_time_window(c: &mut Criterion) {
    let mut group = c.benchmark_group("time_window");
    group.measurement_time(Duration::from_secs(15));

    let buffer_size = 1024;
    let test_buffer = create_test_buffer(buffer_size);
    let num_columns = 10;
    let configs = create_configs(num_columns, buffer_size);

    // Process 600 rows (10 seconds at 60Hz)
    let rows_to_process = 600;

    // Test different time windows
    let window_sizes = vec![1, 5, 10, 30, 60]; // in seconds

    for &window_size in &window_sizes {
        group.bench_with_input(
            BenchmarkId::new("window_seconds", window_size),
            &window_size,
            |b, &window| {
                let mut manager = AccumulatorManager::new(configs.clone(), vec![], 1000);

                // Process the specified number of rows before benchmarking
                for _ in 0..rows_to_process {
                    manager.process_buffer(&test_buffer).unwrap();
                }

                // Select half the columns
                let columns: Vec<usize> = (0..num_columns / 2).collect();

                // Benchmark only the dataframe creation with different window sizes
                b.iter(|| {
                    black_box(manager._get_dataframe(&columns, window).unwrap());
                });

                manager.shutdown();
            },
        );
    }

    group.finish();
}

// Benchmark different column counts for dataframe creation
fn bench_column_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("column_count_dataframe");
    group.measurement_time(Duration::from_secs(15));

    let buffer_size = 1024;
    let test_buffer = create_test_buffer(buffer_size);
    let rows_to_process = 300; // 5 seconds at 60Hz

    // Test different column counts
    let column_counts = vec![10, 50, 100, 500, 1000];

    for &col_count in &column_counts {
        group.bench_with_input(
            BenchmarkId::new("columns", col_count),
            &col_count,
            |b, &cols| {
                let configs = create_configs(cols, buffer_size);
                let mut manager = AccumulatorManager::new(configs.clone(), vec![], 1000);

                // Process data before benchmarking
                for _ in 0..rows_to_process {
                    manager.process_buffer(&test_buffer).unwrap();
                }

                // Select 10% of the columns or at least 5
                let columns_to_fetch = std::cmp::max(5, cols / 10);
                let columns: Vec<usize> = (0..columns_to_fetch).collect();

                // Benchmark only the dataframe creation
                b.iter(|| {
                    black_box(manager._get_dataframe(&columns, 5).unwrap());
                });

                manager.shutdown();
            },
        );
    }

    group.finish();
}

// Ultimate test focusing only on dataframe creation
// TODO This is measuring the accumulator and the dataframe creation.
// this probably duplicates the columns/X benchmark.
fn bench_ultimate_test(c: &mut Criterion) {
    let mut group = c.benchmark_group("ultimate_test_dataframe");

    // Configure for a longer, more intensive benchmark
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(10); // Reduce samples due to benchmark intensity

    // Create the maximum-sized buffer
    let buffer_size = 55 * 1024;
    let test_buffer = create_test_buffer(buffer_size);

    // 2000 columns/sparse accumulators
    let num_columns = 2000;
    let configs = create_configs(num_columns, buffer_size);

    group.bench_function("max_load", |b| {
        let mut manager =
            AccumulatorManager::new_with_params(configs.clone(), vec![], 1000, buffer_size, 120);

        // Process 120 seconds of data at 60Hz = 7200 buffers before benchmarking
        for _ in 0..7200 {
            manager.process_buffer(&test_buffer).unwrap();
        }
        let mut rng = rand::rng();
        // Query for 1000 random columns from the last 120 seconds
        let mut columns: Vec<usize> = (0..1000)
            .map(|_| rng.random_range(0..num_columns))
            .collect();
        columns.sort(); // Sort to avoid duplicates in the benchmark
        columns.dedup();

        // Only benchmark the dataframe creation
        b.iter(|| {
            black_box(manager._get_dataframe(&columns, 120).unwrap());
        });

        manager.shutdown();
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_get_dataframe,
    bench_time_window,
    bench_column_count,
    bench_ultimate_test,
);
criterion_main!(benches);
