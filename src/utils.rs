use std::collections::HashMap;

use super::ieee_c37_118::frames::ConfigurationFrame;
use crate::accumulator::manager::AccumulatorConfig;
use crate::ieee_c37_118::models::{ChannelDataType, ChannelInfo};

use arrow::datatypes::DataType;

pub fn config_to_accumulators(config: &dyn ConfigurationFrame) -> Vec<AccumulatorConfig> {
    // Takes a IEEE c37.118 configuration frame struct and converts it to a vector of Accumulator structs
    // Loops through vec of ChannelDataType  and returns a vector of Accumulator structs.
    //
    // Step 1: Get the accumulator config for the timestamp variable.
    //

    let mut accumulators: Vec<AccumulatorConfig> = vec![AccumulatorConfig {
        var_loc: 6,
        var_len: 8,
        var_type: DataType::Int64,
        name: "DATETIME".to_string(),
    }];

    // Step 2: Get the accumulator configs for everything else
    let channels: HashMap<String, ChannelInfo> = config.get_channel_map();

    for (key, value) in &channels {
        let acc_configs = channel_to_accumulator(value, key);
        // append to accumulators
        accumulators.extend(acc_configs);
    }
    accumulators
}

pub fn channel_to_accumulator(channel: &ChannelInfo, name: &str) -> Vec<AccumulatorConfig> {
    // Takes a ChannelDataType struct and converts it to an one or two Accumulator structs

    // Phasor Float and PhasorFixed get split into two accumulators. Magnitude, Angle or X,Y.
    // If phasor float or fixed, the names shall be modified to add _magnitude,_angle or _x, _y
    //
    match channel.data_type {
        ChannelDataType::AnalogFloat | ChannelDataType::FreqFloat | ChannelDataType::DfreqFloat => {
            vec![AccumulatorConfig {
                var_loc: channel.offset as u16,
                var_len: channel.size as u8,
                var_type: DataType::Float32,
                name: name.to_string(),
            }]
        }
        ChannelDataType::FreqFixed | ChannelDataType::DfreqFixed | ChannelDataType::AnalogFixed => {
            vec![AccumulatorConfig {
                var_loc: channel.offset as u16,
                var_len: channel.size as u8,
                var_type: DataType::Int16,
                name: name.to_string(),
            }]
        }
        ChannelDataType::Digital => {
            vec![AccumulatorConfig {
                var_loc: channel.offset as u16,
                var_len: channel.size as u8,
                var_type: DataType::UInt16,
                name: name.to_string(),
            }]
        }

        // Split the Phasor Float polar into magnitude and angle accumulators.
        ChannelDataType::PhasorFloatPolar => vec![
            AccumulatorConfig {
                var_loc: channel.offset as u16,
                var_len: 4 as u8,
                var_type: DataType::Float32,
                name: format!("{}_magnitude", name),
            },
            AccumulatorConfig {
                var_loc: (channel.offset + 4) as u16,
                var_len: 4 as u8,
                var_type: DataType::Float32,
                name: format!("{}_angle", name),
            },
        ],
        // Split the Phasor Float polar into magnitude and angle accumulators.
        ChannelDataType::PhasorFloatRectangular => vec![
            AccumulatorConfig {
                var_loc: channel.offset as u16,
                var_len: 4 as u8,
                var_type: DataType::Float32,
                name: format!("{}_real", name),
            },
            AccumulatorConfig {
                var_loc: (channel.offset + 4) as u16,
                var_len: 4 as u8,
                var_type: DataType::Float32,
                name: format!("{}_imaginary", name),
            },
        ],

        // Split the Phasor Float rectangular into real and imaginary accumulators
        ChannelDataType::PhasorIntPolar => vec![
            AccumulatorConfig {
                var_loc: channel.offset as u16,
                var_len: 2 as u8,
                var_type: DataType::UInt16,
                name: format!("{}_magnitude", name),
            },
            AccumulatorConfig {
                var_loc: (channel.offset + 2) as u16,
                var_len: 2 as u8,
                var_type: DataType::Int16,
                name: format!("{}_angle", name),
            },
        ],

        // Split the Phasor Float rectangular into real and imaginary accumulators
        ChannelDataType::PhasorIntRectangular => vec![
            AccumulatorConfig {
                var_loc: channel.offset as u16,
                var_len: 2 as u8,
                var_type: DataType::Int16,
                name: format!("{}_real", name),
            },
            AccumulatorConfig {
                var_loc: (channel.offset + 2) as u16,
                var_len: 2 as u8,
                var_type: DataType::Int16,
                name: format!("{}_imaginary", name),
            },
        ],
    }
}
