// This file contains benchmarks to test the throughput of the sparse accumulator manager.
//
// For this test, we should measure the throughput of the sparse accumulator manager for different numbers of SparseAccumulator instances.
//
// We should see how well the sparse accumulator manager performs when managing a large number of instances across various thread counts.
//
// We should benchmark to see that the manager performs well when users are calling get_dataframe while the manager is processing buffers.
//
// Ultimate test: A 55 KB buffer at 240hz and 2000 sparse accumulators. We want to make sure it can handle the 240hz buffer with 2000 sparse accumulators.

use arrow::datatypes::DataType;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rtpa_core::accumulator::manager::{AccumulatorConfig, AccumulatorManager};
use std::thread;
use std::time::{Duration, Instant};

// Helper function to create a test buffer of specified size
fn create_test_buffer(size: usize) -> Vec<u8> {
    let mut buffer = vec![0u8; size];
    for i in 0..size {
        buffer[i] = (i % 256) as u8;
    }
    buffer
}

// Helper function to create a set of configs with varying numbers of columns
fn create_configs(num_columns: usize) -> Vec<AccumulatorConfig> {
    let mut configs = Vec::with_capacity(num_columns);

    for i in 0..num_columns {
        let var_type = match i % 3 {
            0 => DataType::Float32,
            1 => DataType::Int32,
            _ => DataType::UInt16,
        };

        let var_len = match var_type {
            DataType::Float32 => 4,
            DataType::Int32 => 4,
            DataType::UInt16 => 2,
            _ => panic!("Unsupported data type"),
        };

        configs.push(AccumulatorConfig {
            var_loc: ((i * 4) % 50000) as u16, // Space out the variables in the buffer
            var_len,
            var_type,
            name: format!("column_{}", i),
        });
    }

    configs
}

// Benchmark for processing throughput with different numbers of accumulators
fn bench_processing_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("processing_throughput");
    group.measurement_time(Duration::from_secs(15));

    // Create a 55KB buffer for standard tests
    let buffer_size = 55 * 1024;
    let test_buffer = create_test_buffer(buffer_size);

    // Test different numbers of sparse accumulators

    for &num_accumulators in &[100, 500, 1000, 2000] {
        // Set the throughput to be measured in elements per second
        group.throughput(Throughput::Elements(1));

        group.bench_with_input(
            BenchmarkId::new("accumulators", num_accumulators),
            &num_accumulators,
            |b, &num_acc| {
                let configs = create_configs(num_acc);
                let manager =
                    AccumulatorManager::new_with_params(configs, 2, 1000, buffer_size, 120);

                // Measure how many buffers we can process per second
                let duration_seconds = 1.0; // Measure over 1 second

                b.iter_custom(|iters| {
                    let start = Instant::now();
                    let mut count = 0;

                    while start.elapsed().as_secs_f64() < duration_seconds && count < iters {
                        manager
                            .process_buffer(|buffer| {
                                let len = std::cmp::min(buffer.len(), test_buffer.len());
                                buffer[..len].copy_from_slice(&test_buffer[..len]);
                                len
                            })
                            .unwrap();
                        count += 1;
                    }

                    let elapsed = start.elapsed();
                    let throughput = count as f64 / elapsed.as_secs_f64(); // buffers per second

                    // Return the time it would take to process iters buffers at this rate
                    Duration::from_secs_f64(iters as f64 / throughput)
                });

                manager.shutdown();
            },
        );
    }

    group.finish();
}

// Benchmark for thread scaling with fixed number of accumulators
fn bench_thread_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("thread_scaling");
    group.measurement_time(Duration::from_secs(15));

    let buffer_size = 55 * 1024;
    let test_buffer = create_test_buffer(buffer_size);
    let num_accumulators = 2000; // Fixed large number of accumulators

    for &thread_count in &[1, 2, 4, 8] {
        group.throughput(Throughput::Elements(1));

        group.bench_with_input(
            BenchmarkId::new("threads", thread_count),
            &thread_count,
            |b, &threads| {
                let configs = create_configs(num_accumulators);
                let manager =
                    AccumulatorManager::new_with_params(configs, threads, 1000, buffer_size, 120);

                // Measure how many buffers we can process per second
                let duration_seconds = 1.0;

                b.iter_custom(|iters| {
                    let start = Instant::now();
                    let mut count = 0;

                    while start.elapsed().as_secs_f64() < duration_seconds && count < iters {
                        manager
                            .process_buffer(|buffer| {
                                let len = std::cmp::min(buffer.len(), test_buffer.len());
                                buffer[..len].copy_from_slice(&test_buffer[..len]);
                                len
                            })
                            .unwrap();
                        count += 1;
                    }

                    let elapsed = start.elapsed();
                    let throughput = count as f64 / elapsed.as_secs_f64();

                    Duration::from_secs_f64(iters as f64 / throughput)
                });

                manager.shutdown();
            },
        );
    }

    group.finish();
}

// Benchmark for concurrent processing and querying
fn bench_concurrent_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_operations");
    group.measurement_time(Duration::from_secs(20));

    let buffer_size = 55 * 1024;
    let test_buffer = create_test_buffer(buffer_size);

    // Test with different numbers of accumulators
    for &num_accumulators in &[100, 500, 1000] {
        group.throughput(Throughput::Elements(1));

        group.bench_with_input(
            BenchmarkId::new("concurrent_acc", num_accumulators),
            &num_accumulators,
            |b, &num_acc| {
                let configs = create_configs(num_acc);

                b.iter_custom(|iters| {
                    // Create a fresh manager for each iteration to avoid thread issues
                    let manager = AccumulatorManager::new_with_params(configs.clone(), 4, 1000, buffer_size, 120);

                    // First, load some data
                    for _ in 0..600 { // 10 seconds of data at 60Hz
                        manager
                            .process_buffer(|buffer| {
                                let len = std::cmp::min(buffer.len(), test_buffer.len());
                                buffer[..len].copy_from_slice(&test_buffer[..len]);
                                len
                            })
                            .unwrap();
                    }

                    // We'll simulate concurrent access by alternating between
                    // processing and querying in a single thread, to avoid
                    // Sync issues with the AccumulatorManager
                    let start = Instant::now();
                    let mut process_count = 0;
                    let columns: Vec<usize> = (0..num_acc.min(10)).collect();

                    while process_count < iters {
                        // Process a buffer
                        manager
                            .process_buffer(|buffer| {
                                let len = std::cmp::min(buffer.len(), test_buffer.len());
                                buffer[..len].copy_from_slice(&test_buffer[..len]);
                                len
                            })
                            .unwrap();
                        process_count += 1;

                        // Query data every 10 processing operations
                        if process_count % 10 == 0 {
                            let _ = manager._get_dataframe(&columns, 5).unwrap();
                        }
                    }

                    let elapsed = start.elapsed();
                    let effective_rate = process_count as f64 / elapsed.as_secs_f64();

                    // Report effective rate
                    eprintln!(
                        "Effective rate with {} accumulators and intermittent queries: {:.2} buffers/sec",
                        num_acc,
                        effective_rate
                    );

                    manager.shutdown();
                    elapsed
                });
            },
        );
    }

    group.finish();
}

// The ultimate throughput test
fn bench_ultimate_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("ultimate_throughput");
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(15); // Fewer samples due to the intensity

    // Create a 55KB buffer
    let buffer_size = 55 * 1024;
    let test_buffer = create_test_buffer(buffer_size);

    // 2000 sparse accumulators
    let num_accumulators = 2000;
    let configs = create_configs(num_accumulators);

    group.throughput(Throughput::Elements(1));

    group.bench_function("ultimate_240hz", |b| {
        // Use 8 threads to handle the large workload
        let manager =
            AccumulatorManager::new_with_params(configs.clone(), 2, 2000, buffer_size, 120);

        // Warm up
        for _ in 0..100 {
            manager
                .process_buffer(|buffer| {
                    let len = std::cmp::min(buffer.len(), test_buffer.len());
                    buffer[..len].copy_from_slice(&test_buffer[..len]);
                    len
                })
                .unwrap();
        }

        // Measure if we can sustain 240Hz (240 buffers per second)
        let target_rate = 240.0; // Hz
        let measuring_duration = 5.0; // seconds

        b.iter_custom(|_| {
            let start = Instant::now();
            let target_buffers = (target_rate as f64 * measuring_duration as f64).ceil() as u32;
            let frame_time = Duration::from_secs_f64(1.0 / target_rate);

            let mut next_frame = start;
            let mut count = 0;

            // Try to process at exactly 240Hz
            while count < target_buffers {
                // Sleep until the next frame is due
                let now = Instant::now();
                if next_frame > now {
                    let sleep_time = next_frame - now;
                    thread::sleep(sleep_time);
                }

                // Process buffer
                manager
                    .process_buffer(|buffer| {
                        let len = std::cmp::min(buffer.len(), test_buffer.len());
                        buffer[..len].copy_from_slice(&test_buffer[..len]);
                        len
                    })
                    .unwrap();

                count += 1;
                next_frame = start + frame_time * count;
            }

            let elapsed = start.elapsed();
            let achieved_rate = count as f64 / elapsed.as_secs_f64();

            // Report achieved rate
            eprintln!(
                "Target rate: {:.1} Hz, Achieved rate: {:.1} Hz ({:.1}%)",
                target_rate,
                achieved_rate,
                100.0 * achieved_rate / target_rate
            );

            elapsed
        });

        manager.shutdown();
    });

    group.finish();
}

// Benchmark for buffer size impact on throughput
fn bench_buffer_size_impact(c: &mut Criterion) {
    let mut group = c.benchmark_group("buffer_size_impact");
    group.measurement_time(Duration::from_secs(15));

    // Test various buffer sizes
    let buffer_sizes = [4 * 1024, 16 * 1024, 55 * 1024];
    let num_accumulators = 500;

    for &size in &buffer_sizes {
        let test_buffer = create_test_buffer(size);

        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new("buffer_size", size), &size, |b, _| {
            let configs = create_configs(num_accumulators);
            let manager = AccumulatorManager::new_with_params(configs, 2, 1000, size, 120);

            b.iter_custom(|iters| {
                let start = Instant::now();
                let mut count = 0;

                while count < iters {
                    manager
                        .process_buffer(|buffer| {
                            let len = std::cmp::min(buffer.len(), test_buffer.len());
                            buffer[..len].copy_from_slice(&test_buffer[..len]);
                            len
                        })
                        .unwrap();
                    count += 1;
                }

                start.elapsed()
            });

            manager.shutdown();
        });
    }

    group.finish();
}

// Benchmark sustained throughput over time to detect performance degradation
fn bench_sustained_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("sustained_throughput");
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(15); // Fewer samples for longer tests

    let buffer_size = 55 * 1024;
    let test_buffer = create_test_buffer(buffer_size);
    let num_accumulators = 500;

    group.throughput(Throughput::Elements(1));

    group.bench_function("sustained_1min", |b| {
        let configs = create_configs(num_accumulators);
        let manager = AccumulatorManager::new_with_params(configs, 4, 10000, buffer_size, 120); // Large batch capacity

        b.iter_custom(|_| {
            let duration = Duration::from_secs(60); // 1 minute
            let start = Instant::now();
            let mut count = 0;

            // Process buffers for the full duration
            while start.elapsed() < duration {
                manager
                    .process_buffer(|buffer| {
                        let len = std::cmp::min(buffer.len(), test_buffer.len());
                        buffer[..len].copy_from_slice(&test_buffer[..len]);
                        len
                    })
                    .unwrap();
                count += 1;

                // Every 5000 buffers, report the current throughput
                if count % 50000 == 0 {
                    let elapsed = start.elapsed();
                    let rate = count as f64 / elapsed.as_secs_f64();
                    eprintln!(
                        "Current throughput at {} buffers: {:.1} buffers/sec",
                        count, rate
                    );
                }
            }

            let elapsed = start.elapsed();
            let final_rate = count as f64 / elapsed.as_secs_f64();
            eprintln!("Final throughput: {:.1} buffers/sec", final_rate);

            elapsed
        });

        manager.shutdown();
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_processing_throughput,
    bench_thread_scaling,
    bench_concurrent_operations,
    bench_buffer_size_impact,
    bench_sustained_throughput,
    bench_ultimate_throughput,
);
criterion_main!(benches);
