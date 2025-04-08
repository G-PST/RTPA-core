// benches/parsing.rs
#![allow(unused)]
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rtpa_core::frame_parser::{parse_config_frame_1and2, parse_data_frames};
use rtpa_core::frames::{
    calculate_crc, ConfigurationFrame1and2_2011, DataFrame2011, PMUConfigurationFrame2011,
    PMUFrameType, PrefixFrame2011,
};
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

fn read_hex_file(file_name: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let path = Path::new("tests/test_data").join(file_name);
    let content = fs::read_to_string(path)?;
    let hex_string: String = content.chars().filter(|c| !c.is_whitespace()).collect();

    hex_string
        .as_bytes()
        .chunks(2)
        .map(|chunk| {
            let hex_byte = std::str::from_utf8(chunk).unwrap();
            u8::from_str_radix(hex_byte, 16).map_err(|e| e.into())
        })
        .collect()
}

fn benchmark_parse_data_frame(c: &mut Criterion) {
    // Load test data
    let config_buffer = read_hex_file("config_message.bin").unwrap();
    let data_buffer = read_hex_file("data_message.bin").unwrap();

    // Parse configuration once
    let config_frame = parse_config_frame_1and2(&config_buffer).unwrap();

    // Benchmark data frame parsing
    c.bench_function("parse_data_frame", |b| {
        b.iter(|| parse_data_frames(black_box(&data_buffer), black_box(&config_frame)).unwrap());
    });
}

fn benchmark_parse_multiple_frames(c: &mut Criterion) {
    let config_buffer = read_hex_file("config_message.bin").unwrap();
    let data_buffer = read_hex_file("data_message.bin").unwrap();
    let config_frame = parse_config_frame_1and2(&config_buffer).unwrap();

    // Create a buffer with multiple frames
    let mut multi_frame_buffer = Vec::new();
    for _ in 0..10000 {
        multi_frame_buffer.extend_from_slice(&data_buffer);
    }

    c.bench_function("parse_100_frames", |b| {
        b.iter(|| {
            for chunk in multi_frame_buffer.chunks(data_buffer.len()) {
                parse_data_frames(black_box(chunk), black_box(&config_frame)).unwrap();
            }
        });
    });
}
