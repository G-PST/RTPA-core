use crate::frames::{
    AnalogFixed, AnalogFloat, ChannelDataType, ConfigurationFrame, DfreqFixed, DfreqFloat, Digital,
    FreqFixed, FreqFloat, PhasorFixed, PhasorFloat, PrefixFrame,
};

use crate::accumulators::sparse::{Accumulator, AccumulatorType};

pub fn config_to_accumulators() -> Vec<Accumulator> {
    // Takes a IEEE c37.118 configuration frame struct and converts it to a vector of Accumulator structs
    // Loops through vec of ChannelDataType  and returns a vector of Accumulator structs.
    vec![]
}

pub fn channel_to_accumulator(
    channel: ChannelDataType,
    offset: usize,
    name: &str,
) -> Vec<Accumulator> {
    // Takes a ChannelDataType struct and converts it to an one or two Accumulator structs

    // Phasor Float and PhasorFixed get split into two accumulators. Magnitude, Angle or X,Y.
    // If phasor float or fixed, the names shall be modified to add _magnitude,_angle or _x, _y
    //
    match channel {
        ChannelDataType::PhasorFloat(_) => vec![Accumulator::F32(AccumulatorType::PhasorFloat)],
        ChannelDataType::PhasorFixed(_) => vec![Accumulator::I16(AccumulatorType::PhasorFixed)],

        _ => vec![],
    }
}
