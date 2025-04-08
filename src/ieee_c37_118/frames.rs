use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;

/// Represents errors that can occur during parsing
#[derive(Debug)]
pub enum ParseError {
    InvalidLength,
    InvalidFrameType,
    InvalidChecksum,
    InvalidFormat,
    InvalidHeader,
    // Add more specific error types as needed
}

/// Represents the type of the frame
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameType {
    Data,
    Header,
    Config1,
    Config2,
    Config3,
    Command,
}

/// Common trait for all frame types
pub trait Frame {
    /// Get the frame type
    fn frame_type(&self) -> FrameType;

    /// Get the ID code of the frame
    fn id_code(&self) -> u16;

    /// Get the size of the frame in bytes
    fn frame_size(&self) -> u16;

    /// Validate the checksum of the frame
    fn validate_checksum(&self, data: &[u8]) -> bool;

    /// Convert to Any for downcasting
    fn as_any(&self) -> &dyn Any;
}

/// Trait for all prefix frames
pub trait PrefixFrame {
    /// Get the sync value
    fn sync(&self) -> u16;

    /// Get the frame size
    fn frame_size(&self) -> u16;

    /// Get the ID code
    fn id_code(&self) -> u16;

    /// Get the SOC (second of century)
    fn soc(&self) -> u32;

    /// Get the fraction of second
    fn fracsec(&self) -> u32;

    /// Get the frame type from the sync byte
    fn frame_type(&self) -> FrameType;

    /// Create a new prefix frame from bytes
    fn from_bytes(bytes: &[u8]) -> Result<Self, ParseError>
    where
        Self: Sized;

    /// Convert the prefix frame to bytes
    fn to_bytes(&self) -> Vec<u8>;

    fn to_concrete_frame(&self) -> crate::ieee_c37_118::frames_v2::PrefixFrame2011;

    fn as_any(&self) -> &dyn Any;
}

/// Trait for configuration frames
pub trait ConfigurationFrame: Frame {
    /// Get the time base value
    fn time_base(&self) -> u32;

    /// Get the number of PMUs
    fn num_pmu(&self) -> u16;

    /// Get the data rate
    fn data_rate(&self) -> i16;

    /// Calculate the expected data frame size
    fn calc_data_frame_size(&self) -> usize;

    /// Get a mapping of channels to their information
    fn get_channel_map(&self) -> HashMap<String, ChannelInfo>;

    /// Get the prefix frame as trait object
    fn prefix(&self) -> &dyn PrefixFrame;

    /// Create a new configuration frame from bytes
    fn from_bytes(bytes: &[u8]) -> Result<Self, ParseError>
    where
        Self: Sized;
}

/// Trait for command frames
pub trait CommandFrame: Frame {
    /// Create a new command to turn off transmission
    fn new_turn_off_transmission(id_code: u16) -> Self
    where
        Self: Sized;

    /// Create a new command to turn on transmission
    fn new_turn_on_transmission(id_code: u16) -> Self
    where
        Self: Sized;

    /// Create a new command to send header frame
    fn new_send_header_frame(id_code: u16) -> Self
    where
        Self: Sized;

    /// Create a new command to send config frame 1
    fn new_send_config_frame1(id_code: u16) -> Self
    where
        Self: Sized;

    /// Create a new command to send config frame 2
    fn new_send_config_frame2(id_code: u16) -> Self
    where
        Self: Sized;

    /// Create a new command to send config frame 3
    fn new_send_config_frame3(id_code: u16) -> Self
    where
        Self: Sized;

    /// Create a new extended frame command
    fn new_extended_frame(id_code: u16) -> Self
    where
        Self: Sized;

    /// Convert to bytes
    fn to_bytes(&self) -> Vec<u8>;

    /// Get the command value
    fn command(&self) -> u16;

    /// Get the prefix frame as trait object
    fn prefix(&self) -> &dyn PrefixFrame;

    /// Create a new command frame from bytes
    fn from_bytes(bytes: &[u8]) -> Result<Self, ParseError>
    where
        Self: Sized;
}

/// Trait for data frames
pub trait DataFrame: Frame {
    /// Get the prefix frame
    fn prefix(&self) -> &dyn PrefixFrame;

    /// Get a value from the data frame by channel name
    fn get_value(&self, channel_name: &str, config: &dyn ConfigurationFrame) -> Option<DataValue>;

    /// Get all values from the data frame
    fn get_all_values(&self, config: &dyn ConfigurationFrame) -> HashMap<String, DataValue>;

    /// Create a new data frame from bytes using configuration information
    fn from_bytes(bytes: &[u8], config: &dyn ConfigurationFrame) -> Result<Self, ParseError>
    where
        Self: Sized;
}

/// Represents a channel's information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelInfo {
    pub data_type: ChannelDataType,
    pub offset: usize,
    pub size: usize, // TODO remove if not necessary. (it shouldn't be for accumulator but might for single frame deserialization.)
}

/// Represents the data type of a channel
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ChannelDataType {
    // Need to add whether these are polar or not.
    PhasorFloatRectangular,
    PhasorFloatPolar,
    PhasorIntRectangular,
    PhasorIntPolar,
    AnalogFloat,
    AnalogFixed,
    Digital,
    FreqFloat,
    FreqFixed,
    DfreqFloat,
    DfreqFixed,
}

/// Represents a value from a data frame
#[derive(Debug, Clone)]
pub enum DataValue {
    PhasorFloatRectangular { x: f32, y: f32 },
    PhasorFloatPolar { magnitude: f32, angle: f32 },
    PhasorIntRectangular { x: i16, y: i16 },
    PhasorIntPolar { magnitude: u16, angle: i16 }, // Angle is in radians * 10^4.
    Float(f32),
    Fixed(i16),
    Digital(u16),
    Integer(i32),
}
