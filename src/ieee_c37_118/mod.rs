#![allow(unused)]

// Public modules
pub mod frames;
pub mod models;
pub mod utils;
// Version-specific implementations
pub mod frames_v2;

// Implementation of generic traits for each version
mod frames_v2_impl;

// Test module that will only be compiled when running tests
#[cfg(test)]
mod tests;

// Re-export version-agnostic interfaces
pub use frames::{
    CommandFrame, ConfigurationFrame, DataFrame, Frame, FrameType, ParseError, PrefixFrame,
};

// Factory functions to create the appropriate frame based on protocol version
pub fn create_configuration_frame(bytes: &[u8]) -> Result<Box<dyn ConfigurationFrame>, ParseError> {
    if bytes.len() < 2 {
        return Err(ParseError::InvalidLength);
    }

    let sync = u16::from_be_bytes([bytes[0], bytes[1]]);
    let version = utils::get_protocol_version(sync);

    // Protocol version should be 2 for IEEE C37.118.2-2011
    // If it's not, we need to adjust our logic
    match version {
        2 => <frames_v2::ConfigurationFrame1and2_2011 as ConfigurationFrame>::from_bytes(bytes)
            .map(|frame| Box::new(frame) as Box<dyn ConfigurationFrame>),
        // Temporary workaround - if version is not 2, try version 2 anyway
        _ => <frames_v2::ConfigurationFrame1and2_2011 as ConfigurationFrame>::from_bytes(bytes)
            .map(|frame| Box::new(frame) as Box<dyn ConfigurationFrame>),
    }
}

pub fn create_command_frame(bytes: &[u8]) -> Result<Box<dyn CommandFrame>, ParseError> {
    if bytes.len() < 2 {
        println!(
            "Error: Command frame buffer too short: {} bytes",
            bytes.len()
        );
        return Err(ParseError::InvalidLength);
    }

    let sync = u16::from_be_bytes([bytes[0], bytes[1]]);
    let version = utils::get_protocol_version(sync);
    println!("Command frame: sync=0x{:04X}, version={}", sync, version);

    match version {
        1 | 2 => {
            // Try to parse as CommandFrame2011
            match <frames_v2::CommandFrame2011 as CommandFrame>::from_bytes(bytes) {
                Ok(frame) => Ok(Box::new(frame) as Box<dyn CommandFrame>),
                Err(err) => {
                    println!("Error parsing CommandFrame2011: {:?}", err);
                    // Try to get more info about the error
                    if bytes.len() >= 18 {
                        let framesize = u16::from_be_bytes([bytes[2], bytes[3]]);
                        let idcode = u16::from_be_bytes([bytes[4], bytes[5]]);
                        let command = u16::from_be_bytes([bytes[14], bytes[15]]);
                        println!(
                            "Frame details: size={}, idcode={}, command={}",
                            framesize, idcode, command
                        );

                        // Check checksum
                        let is_valid = utils::validate_checksum(bytes);
                        println!("Checksum valid: {}", is_valid);
                    }
                    Err(err)
                }
            }
        }
        _ => {
            println!("Unsupported protocol version: {}", version);
            Err(ParseError::InvalidFrameType)
        }
    }
}

pub fn create_data_frame(
    bytes: &[u8],
    config: &dyn ConfigurationFrame,
) -> Result<Box<dyn DataFrame>, ParseError> {
    if bytes.len() < 2 {
        return Err(ParseError::InvalidLength);
    }

    let sync = u16::from_be_bytes([bytes[0], bytes[1]]);
    let version = utils::get_protocol_version(sync);

    match version {
        1 | 2 => {
            // Use fully qualified path for the implementation
            <frames_v2::DataFrame2011 as DataFrame>::from_bytes(bytes, config)
                .map(|frame| Box::new(frame) as Box<dyn DataFrame>)
        }
        _ => Err(ParseError::InvalidFrameType),
    }
}
