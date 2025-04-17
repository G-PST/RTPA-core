use crate::frames::{ChannelDataType, ChannelInfo};
use arrow::array::{ArrayRef, Float32Array, Int16Array, UInt16Array};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::collections::HashMap;
use std::sync::Arc;

pub fn build_arrow_schema(channel_map: &HashMap<String, ChannelInfo>) -> Schema {
    let mut fields = vec![Field::new(
        "timestamp",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        false,
    )];

    for (name, info) in channel_map {
        match info.data_type {
            ChannelDataType::PhasorFloat => {
                fields.push(Field::new(
                    &format!("{}_magnitude", name),
                    DataType::Float32,
                    false,
                ));
                fields.push(Field::new(
                    &format!("{}_angle", name),
                    DataType::Float32,
                    false,
                ));
            }
            ChannelDataType::PhasorFixed => {
                fields.push(Field::new(&format!("{}_X", name), DataType::Int16, false));
                fields.push(Field::new(&format!("{}_Y", name), DataType::Int16, false));
            }
            ChannelDataType::AnalogFloat
            | ChannelDataType::FreqFloat
            | ChannelDataType::DfreqFloat => {
                fields.push(Field::new(name, DataType::Float32, false));
            }
            ChannelDataType::AnalogFixed
            | ChannelDataType::FreqFixed
            | ChannelDataType::DfreqFixed => {
                fields.push(Field::new(name, DataType::Int16, false));
            }
            ChannelDataType::Digital => {
                fields.push(Field::new(name, DataType::UInt16, false));
            }
        }
    }

    Schema::new(fields)
}

fn extract_float32_values(
    buffer: &[u8],
    frame_size: usize,
    channel_info: &ChannelInfo,
) -> Float32Array {
    let mut values = Vec::new();
    for frame in buffer.chunks(frame_size) {
        if frame.len() < frame_size {
            break;
        }
        let data_start = channel_info.offset;
        let data_end = data_start + channel_info.size;
        if data_end <= frame.len() {
            let data = &frame[data_start..data_end];
            let value = f32::from_be_bytes(data.try_into().unwrap());
            values.push(value);
        }
    }
    Float32Array::from(values)
}

fn extract_int16_values(
    buffer: &[u8],
    frame_size: usize,
    channel_info: &ChannelInfo,
) -> Int16Array {
    let mut values = Vec::new();
    for frame in buffer.chunks(frame_size) {
        if frame.len() < frame_size {
            break;
        }
        let data_start = channel_info.offset;
        let data_end = data_start + channel_info.size;
        if data_end <= frame.len() {
            let data = &frame[data_start..data_end];
            let value = i16::from_be_bytes(data.try_into().unwrap());
            values.push(value);
        }
    }
    Int16Array::from(values)
}

fn extract_uint16_values(
    buffer: &[u8],
    frame_size: usize,
    channel_info: &ChannelInfo,
) -> UInt16Array {
    let mut values = Vec::new();
    for frame in buffer.chunks(frame_size) {
        if frame.len() < frame_size {
            break;
        }
        let data_start = channel_info.offset;
        let data_end = data_start + channel_info.size;
        if data_end <= frame.len() {
            let data = &frame[data_start..data_end];
            let value = u16::from_be_bytes(data.try_into().unwrap());
            values.push(value);
        }
    }
    UInt16Array::from(values)
}

pub fn extract_channel_values(
    buffer: &[u8],
    frame_size: usize,
    channel_info: &ChannelInfo,
) -> Vec<ArrayRef> {
    match channel_info.data_type {
        ChannelDataType::PhasorFloat => {
            let mut values = Vec::new();
            for frame in buffer.chunks(frame_size) {
                if frame.len() < frame_size {
                    break;
                }
                let data_start = channel_info.offset;
                let data_end = data_start + channel_info.size;
                if data_end <= frame.len() {
                    let data = &frame[data_start..data_end];
                    let magnitude = f32::from_be_bytes(data[..4].try_into().unwrap());
                    let angle = f32::from_be_bytes(data[4..].try_into().unwrap());
                    values.push((magnitude, angle));
                }
            }
            vec![
                Arc::new(Float32Array::from(
                    values.iter().map(|(m, _)| *m).collect::<Vec<_>>(),
                )),
                Arc::new(Float32Array::from(
                    values.iter().map(|(_, a)| *a).collect::<Vec<_>>(),
                )),
            ]
        }
        ChannelDataType::PhasorFixed => {
            let mut values = Vec::new();
            for frame in buffer.chunks(frame_size) {
                if frame.len() < frame_size {
                    break;
                }
                let data_start = channel_info.offset;
                let data_end = data_start + channel_info.size;
                if data_end <= frame.len() {
                    let data = &frame[data_start..data_end];
                    let magnitude = i16::from_be_bytes(data[..2].try_into().unwrap());
                    let angle = i16::from_be_bytes(data[2..].try_into().unwrap());
                    values.push((magnitude, angle));
                }
            }
            vec![
                Arc::new(Int16Array::from(
                    values.iter().map(|(m, _)| *m).collect::<Vec<_>>(),
                )),
                Arc::new(Int16Array::from(
                    values.iter().map(|(_, a)| *a).collect::<Vec<_>>(),
                )),
            ]
        }
        ChannelDataType::AnalogFloat | ChannelDataType::FreqFloat | ChannelDataType::DfreqFloat => {
            vec![Arc::new(extract_float32_values(
                buffer,
                frame_size,
                channel_info,
            ))]
        }
        ChannelDataType::AnalogFixed | ChannelDataType::FreqFixed | ChannelDataType::DfreqFixed => {
            vec![Arc::new(extract_int16_values(
                buffer,
                frame_size,
                channel_info,
            ))]
        }
        ChannelDataType::Digital => {
            vec![Arc::new(extract_uint16_values(
                buffer,
                frame_size,
                channel_info,
            ))]
        }
    }
}
