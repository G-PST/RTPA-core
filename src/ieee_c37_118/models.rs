// This file contains the data models for the IEEE C37.118 standard.
//
//
#[derive(Debug, Clone)]
pub enum ChannelDataType {
    PhasorFloatPolar,       // 8 bytes (magnitude + angle as f32)
    PhasorFloatRectangular, // 8 bytes (real + imaginary as f32)
    PhasorIntPolar,         // 8 bytes (magnitude + angle as i32)
    PhasorIntRectangular,   // 8 bytes (real + imaginary as i32)
    AnalogFloat,            // 4 bytes (f32)
    AnalogFixed,            // 2 bytes (i16)
    Digital,                // 2 bytes (u16)
    FreqFloat,              // 4 bytes (f32)
    FreqFixed,              // 2 bytes (i16)
    DfreqFloat,             // 4 bytes (f32)
    DfreqFixed,             // 2 bytes (i16)
}
#[derive(Debug, Clone)]
pub struct ChannelInfo {
    pub data_type: ChannelDataType,
    pub offset: usize, // Offset from start of PMU data section
    pub size: usize,   // Size in bytes
                       // TODO add channel name field here.
}
#[derive(Debug, Clone)]
pub enum DataValue {
    PhasorFloatRectangular { x: f32, y: f32 },
    PhasorFloatPolar { magnitude: f32, angle: f32 },
    PhasorIntRectangular { x: i16, y: i16 },
    PhasorIntPolar { magnitude: u16, angle: i16 }, // Angle is in radians * 10^4.
    // Need to divide by 10^4 to get the angle in radians.
    Float(f32),
    Fixed(i16),
    Digital(u16),
    Integer(i32),
}
