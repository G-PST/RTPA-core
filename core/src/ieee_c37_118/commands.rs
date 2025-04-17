#![allow(unused)]

use super::common::{ParseError, PrefixFrame, Version};
use super::utils::{calculate_crc, validate_checksum};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandFrame {
    pub prefix: PrefixFrame,
    pub command: u16,                   // Command field
    pub extended_data: Option<Vec<u8>>, // Optional extended data
    pub chk: u16,                       // CRC-CCITT checksum
}

/// Command types as defined in IEEE C37.118
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum CommandType {
    TurnOffTransmission = 1,
    TurnOnTransmission = 2,
    SendHeaderFrame = 3,
    SendConfigFrame1 = 4,
    SendConfigFrame2 = 5,
    SendConfigFrame3 = 6,
    SendExtendedFrame = 8,
}

impl CommandFrame {
    /// Parse a command frame from hex bytes
    pub fn from_hex(bytes: &[u8]) -> Result<Self, ParseError> {
        // Validate minimum frame size
        if bytes.len() < 18 {
            // PrefixFrame + command + CHK
            return Err(ParseError::InvalidLength);
        }

        // Validate checksum
        if !validate_checksum(bytes) {
            return Err(ParseError::InvalidChecksum);
        }

        // Parse prefix frame (first 14 bytes)
        let prefix = PrefixFrame::from_hex(&bytes[0..14])?;

        // Check if framesize matches buffer length
        if prefix.framesize as usize != bytes.len() {
            return Err(ParseError::InvalidLength);
        }

        // Get command value
        let command = u16::from_be_bytes([bytes[14], bytes[15]]);

        // Check for extended data
        let extended_data = if bytes.len() > 18 {
            Some(bytes[16..bytes.len() - 2].to_vec())
        } else {
            None
        };

        // Extract checksum from the last two bytes
        let chk = u16::from_be_bytes([bytes[bytes.len() - 2], bytes[bytes.len() - 1]]);

        Ok(CommandFrame {
            prefix,
            command,
            extended_data,
            chk,
        })
    }

    /// Convert the command frame to hex bytes
    pub fn to_hex(&self) -> Vec<u8> {
        let mut result = Vec::new();

        // Add prefix frame
        result.extend_from_slice(&self.prefix.to_hex());

        // Add command field
        result.extend_from_slice(&self.command.to_be_bytes());

        // Add extended data (if any)
        if let Some(data) = &self.extended_data {
            result.extend_from_slice(data);
        }

        // Calculate and add checksum
        let crc = calculate_crc(&result);
        result.extend_from_slice(&crc.to_be_bytes());

        result
    }

    // Factory methods for creating different command types - now using CommandType enum

    /// Create a command to turn off real-time data transmission
    pub fn new_turn_off_transmission(idcode: u16, time: Option<(u32, u32)>) -> Self {
        Self::new_command(idcode, CommandType::TurnOffTransmission, time, None)
    }

    /// Create a command to turn on real-time data transmission
    pub fn new_turn_on_transmission(idcode: u16, time: Option<(u32, u32)>) -> Self {
        Self::new_command(idcode, CommandType::TurnOnTransmission, time, None)
    }

    /// Create a command to request a header frame
    pub fn new_send_header_frame(idcode: u16, time: Option<(u32, u32)>) -> Self {
        Self::new_command(idcode, CommandType::SendHeaderFrame, time, None)
    }

    /// Create a command to request a configuration frame type 1
    pub fn new_send_config_frame1(idcode: u16, time: Option<(u32, u32)>) -> Self {
        Self::new_command(idcode, CommandType::SendConfigFrame1, time, None)
    }

    /// Create a command to request a configuration frame type 2
    pub fn new_send_config_frame2(idcode: u16, time: Option<(u32, u32)>) -> Self {
        Self::new_command(idcode, CommandType::SendConfigFrame2, time, None)
    }

    /// Create a command to request a configuration frame type 3
    pub fn new_send_config_frame3(idcode: u16, time: Option<(u32, u32)>) -> Self {
        Self::new_command(idcode, CommandType::SendConfigFrame3, time, None)
    }

    /// Create a command with extended data
    pub fn new_extended_command(
        idcode: u16,
        time: Option<(u32, u32)>,
        extended_data: Vec<u8>,
    ) -> Self {
        Self::new_command(
            idcode,
            CommandType::SendExtendedFrame,
            time,
            Some(extended_data),
        )
    }

    /// Generic method to create a command - now using CommandType enum
    fn new_command(
        idcode: u16,
        command_type: CommandType,
        time: Option<(u32, u32)>,
        extended_data: Option<Vec<u8>>,
    ) -> Self {
        // Calculate frame size: prefix (14) + command (2) + extended (variable) + checksum (2)
        let ext_size = extended_data.as_ref().map_or(0, |data| data.len());
        let framesize = 14 + 2 + ext_size + 2;

        // Default prefix with command frame sync word
        let (soc, fracsec) = time.unwrap_or((0, 0));

        // For command frames, use 0xAA41 which is the command frame type with version 1
        // First byte is 0xAA, second byte has bits to indicate command frame and version
        let sync = 0xAA41; // Command frame (0x4) with version 1

        let prefix = PrefixFrame {
            sync,
            framesize: framesize as u16,
            idcode,
            soc,
            fracsec,
            version: Version::V2011, // Default to 2011 version
        };

        // Create command frame (checksum will be calculated in to_hex)
        CommandFrame {
            prefix,
            command: command_type as u16,
            extended_data,
            chk: 0, // Placeholder, will be calculated when to_hex() is called
        }
    }

    /// Get the command type as an enum
    pub fn command_type(&self) -> Option<CommandType> {
        match self.command {
            1 => Some(CommandType::TurnOffTransmission),
            2 => Some(CommandType::TurnOnTransmission),
            3 => Some(CommandType::SendHeaderFrame),
            4 => Some(CommandType::SendConfigFrame1),
            5 => Some(CommandType::SendConfigFrame2),
            6 => Some(CommandType::SendConfigFrame3),
            8 => Some(CommandType::SendExtendedFrame),
            _ => None,
        }
    }

    /// Get a description of the command
    pub fn command_description(&self) -> String {
        match self.command_type() {
            Some(cmd_type) => cmd_type.to_string(),
            None => format!("Unknown command ({})", self.command),
        }
    }

    /// Create a new command with any command type
    pub fn new(
        idcode: u16,
        cmd_type: CommandType,
        time: Option<(u32, u32)>,
        extended_data: Option<Vec<u8>>,
    ) -> Self {
        Self::new_command(idcode, cmd_type, time, extended_data)
    }
}

impl std::fmt::Display for CommandType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommandType::TurnOffTransmission => write!(f, "Turn OFF real-time data transmission"),
            CommandType::TurnOnTransmission => write!(f, "Turn ON real-time data transmission"),
            CommandType::SendHeaderFrame => write!(f, "Send Header frame"),
            CommandType::SendConfigFrame1 => write!(f, "Send Configuration frame 1"),
            CommandType::SendConfigFrame2 => write!(f, "Send Configuration frame 2"),
            CommandType::SendConfigFrame3 => write!(f, "Send Configuration frame 3"),
            CommandType::SendExtendedFrame => write!(f, "Send Extended frame"),
        }
    }
}

/// Allow conversion from u16 to CommandType for backward compatibility
impl TryFrom<u16> for CommandType {
    type Error = String;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(CommandType::TurnOffTransmission),
            2 => Ok(CommandType::TurnOnTransmission),
            3 => Ok(CommandType::SendHeaderFrame),
            4 => Ok(CommandType::SendConfigFrame1),
            5 => Ok(CommandType::SendConfigFrame2),
            6 => Ok(CommandType::SendConfigFrame3),
            8 => Ok(CommandType::SendExtendedFrame),
            _ => Err(format!("Invalid command type: {}", value)),
        }
    }
}

#[test]
fn test_command_frame_creation_and_parsing() {
    // Create a command to turn on transmission using the enum
    let cmd_frame = CommandFrame::new_turn_on_transmission(7734, Some((1_149_577_200, 0)));

    // Convert to bytes
    let bytes = cmd_frame.to_hex();

    // Check frame size
    assert_eq!(bytes.len(), 18); // 14 (prefix) + 2 (command) + 2 (checksum)

    // Check common values
    assert_eq!(bytes[0], 0xAA); // First sync byte
    assert_eq!(bytes[1], 0x41); // Command frame type and version bits
    assert_eq!(u16::from_be_bytes([bytes[4], bytes[5]]), 7734); // ID code
    assert_eq!(bytes[14], 0); // Command high byte
    assert_eq!(bytes[15], 2); // Command low byte (turn on)

    // Verify checksum
    assert!(validate_checksum(&bytes));

    // Parse back to a command frame
    let parsed_cmd = CommandFrame::from_hex(&bytes).unwrap();

    // Verify parsed values
    assert_eq!(parsed_cmd.prefix.idcode, 7734);
    assert_eq!(parsed_cmd.command, 2);
    assert_eq!(
        parsed_cmd.command_type(),
        Some(CommandType::TurnOnTransmission)
    );
    assert_eq!(parsed_cmd.extended_data, None);

    // Test with extended data
    let ext_data = vec![0x01, 0x02, 0x03, 0x04];
    let ext_cmd =
        CommandFrame::new_extended_command(7734, Some((1_149_577_200, 0)), ext_data.clone());

    let ext_bytes = ext_cmd.to_hex();

    // Check extended frame size
    assert_eq!(ext_bytes.len(), 18 + ext_data.len());

    // Verify extended data
    assert_eq!(&ext_bytes[16..20], &ext_data[..]);

    // Verify checksum still works with extended data
    assert!(validate_checksum(&ext_bytes));

    // Parse extended command
    let parsed_ext = CommandFrame::from_hex(&ext_bytes).unwrap();
    assert_eq!(parsed_ext.command, 8);
    assert_eq!(parsed_ext.extended_data.unwrap(), ext_data);

    // Test the new generic constructor with command enum
    let generic_cmd = CommandFrame::new(
        7734,
        CommandType::SendConfigFrame1,
        Some((1_149_577_200, 0)),
        None,
    );
    assert_eq!(generic_cmd.command, 4); // SendConfigFrame1 = 4
    assert_eq!(
        generic_cmd.command_type(),
        Some(CommandType::SendConfigFrame1)
    );
}
