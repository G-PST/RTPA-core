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
    VersionStandard,
};

// Factory functions to create the appropriate frame based on protocol version
pub fn parse_configuration_frame(bytes: &[u8]) -> Result<Box<dyn ConfigurationFrame>, ParseError> {
    if bytes.len() < 2 {
        return Err(ParseError::InvalidLength);
    }

    let sync = u16::from_be_bytes([bytes[0], bytes[1]]);
    let frame_type = utils::parse_frame_type(sync).unwrap();
    let version = utils::parse_protocol_version(sync).unwrap();

    // fail early on frame type
    if frame_type != FrameType::Config1
        && frame_type != FrameType::Config2
        && frame_type != FrameType::Config3
    {
        return Err(ParseError::InvalidFrameType);
    }
    // Protocol version should be 2 for IEEE C37.118.2-2011
    // If it's not, we need to adjust our logic
    match version {
        VersionStandard::Ieee2011 => {
            <frames_v2::ConfigurationFrame1and2_2011 as ConfigurationFrame>::from_bytes(bytes)
                .map(|frame| Box::new(frame) as Box<dyn ConfigurationFrame>)
        }
        // Temporary workaround - if version is not 2, try version 2 anyway
        _ => {
            println!(
                "Protocol not currently supported falling back to version 2: {}",
                version
            );
            <frames_v2::ConfigurationFrame1and2_2011 as ConfigurationFrame>::from_bytes(bytes)
                .map(|frame| Box::new(frame) as Box<dyn ConfigurationFrame>)
        }
    }
}

pub fn parse_command_frame(bytes: &[u8]) -> Result<Box<dyn CommandFrame>, ParseError> {
    if bytes.len() < 2 {
        println!(
            "Error: Command frame buffer too short: {} bytes",
            bytes.len()
        );
        return Err(ParseError::InvalidLength);
    }

    let sync = u16::from_be_bytes([bytes[0], bytes[1]]);
    let version = utils::parse_protocol_version(sync).unwrap();
    println!("Command frame: sync=0x{:04X}, version={}", sync, version);

    match version {
        VersionStandard::Ieee2005 | VersionStandard::Ieee2011 => {
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
            Err(ParseError::VersionNotSupported)
        }
    }
}

pub fn serialize_command(command_type: String, version: VersionStandard, id_code: u16) -> Vec<u8> {
    // We need a way to create a serialized command frame
    // based on which type of command we want to send. e.g. send configuration frame 2.
    // or start/stop transmission
    !todo!();
    let mut bytes = Vec::new();
    bytes
}

pub fn parse_data_frame(
    bytes: &[u8],
    config: &dyn ConfigurationFrame,
) -> Result<Box<dyn DataFrame>, ParseError> {
    if bytes.len() < 2 {
        return Err(ParseError::InvalidLength);
    }

    let sync = u16::from_be_bytes([bytes[0], bytes[1]]);
    let version = utils::parse_protocol_version(sync).unwrap();

    match version {
        VersionStandard::Ieee2005 | VersionStandard::Ieee2011 => {
            // Use fully qualified path for the implementation
            <frames_v2::DataFrame2011 as DataFrame>::from_bytes(bytes, config)
                .map(|frame| Box::new(frame) as Box<dyn DataFrame>)
        }
        _ => Err(ParseError::VersionNotSupported),
    }
}
