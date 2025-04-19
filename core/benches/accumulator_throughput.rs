// This file contains benchmarks to test the throughput of the sparse accumulator manager.
//
// For this test, we should measure the throughput of the sparse accumulator manager for different numbers of SparseAccumulator instances.
//
// We should see how well the sparse accumulator manager performs when managing a large number of instances across various thread counts.
//
// We should benchmark to see that the manager performs well when users are calling get_dataframe while the manager is processing buffers.
//
// Ultimate test: A 55 KB buffer at 240hz and 2000 sparse accumulators. We want to make sure it can handle the 240hz buffer with 2000 sparse accumulators.
mod utils;
use utils::{create_configs, create_test_buffer};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rtpa_core::accumulator::manager::AccumulatorManager;
use std::time::{Duration, Instant};

// Benchmark for processing throughput with different numbers of accumulators
fn bench_processing_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("processing_throughput");
    group.measurement_time(Duration::from_secs(15));

    // Create buffers of different sizes for benchmarking
    let buffer_sizes = [4 * 1024, 16 * 1024, 55 * 1024];

    for &buffer_size in &buffer_sizes {
        let test_buffer = create_test_buffer(buffer_size);

        // Test different numbers of sparse accumulators
        for &num_accumulators in &[100, 500, 1000, 2000] {
            group.throughput(Throughput::Bytes(buffer_size as u64));

            group.bench_with_input(
                BenchmarkId::new(
                    format!("buf_{}_acc_{}", buffer_size / 1024, num_accumulators),
                    "",
                ),
                &(buffer_size, num_accumulators),
                |b, &(buffer_size, num_acc)| {
                    let configs = create_configs(num_acc, buffer_size);
                    let mut manager = AccumulatorManager::new_with_params(
                        configs,
                        vec![],
                        1000,
                        buffer_size,
                        120,
                    );

                    // Measure how many buffers we can process per second
                    b.iter(|| {
                        manager.process_buffer(&test_buffer).unwrap();
                    });

                    manager.shutdown();
                },
            );
        }
    }

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
            let configs = create_configs(num_accumulators, size);
            let mut manager = AccumulatorManager::new_with_params(configs, vec![], 1000, size, 120);

            b.iter(|| {
                manager.process_buffer(&test_buffer).unwrap();
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

    group.bench_function("sustained_30sec", |b| {
        let configs = create_configs(num_accumulators, buffer_size);
        let mut manager =
            AccumulatorManager::new_with_params(configs, vec![], 10000, buffer_size, 120); // Large batch capacity

        b.iter_custom(|_| {
            let duration = Duration::from_secs(30); // 30 seconds
            let start = Instant::now();
            let mut count = 0;

            // Process buffers for the full duration
            while start.elapsed() < duration {
                manager.process_buffer(&test_buffer).unwrap();
                count += 1;

                // Every 5000 buffers, report the current throughput
                if count % 5000 == 0 {
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

// The ultimate throughput test
fn bench_ultimate_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("ultimate_throughput");
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(10); // Fewer samples due to the intensity

    // Create a 55KB buffer
    let buffer_size = 55 * 1024;
    let test_buffer = create_test_buffer(buffer_size);

    // 2000 sparse accumulators
    let num_accumulators = 2000;
    let configs = create_configs(num_accumulators, buffer_size);

    group.throughput(Throughput::Elements(1));

    group.bench_function("ultimate_240hz", |b| {
        let mut manager =
            AccumulatorManager::new_with_params(configs.clone(), vec![], 5000, buffer_size, 120);

        // Warm up
        for _ in 0..100 {
            manager.process_buffer(&test_buffer).unwrap();
        }

        // Measure if we can sustain 240Hz (240 buffers per second)
        let target_rate = 240.0; // Hz
        let measuring_duration = 5.0; // seconds

        b.iter_custom(|_| {
            let start = Instant::now();
            let target_buffers = (target_rate * measuring_duration) as u32;

            // Process at maximum speed and see if we can meet or exceed 240Hz
            for _ in 0..target_buffers {
                manager.process_buffer(&test_buffer).unwrap();
            }

            let elapsed = start.elapsed();
            let achieved_rate = target_buffers as f64 / elapsed.as_secs_f64();

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

criterion_group!(
    benches,
    bench_processing_throughput,
    bench_buffer_size_impact,
    bench_sustained_throughput,
    bench_ultimate_throughput,
);
criterion_main!(benches);
