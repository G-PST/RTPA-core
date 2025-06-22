#![allow(unused)]
use log::{error, info};
use std::error::Error;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::{self, Duration};

use rtpa_core::ieee_c37_118::commands::{CommandFrame, CommandType};
use rtpa_core::ieee_c37_118::common::{PrefixFrame, Version};
use rtpa_core::ieee_c37_118::config::ConfigurationFrame;
use rtpa_core::ieee_c37_118::data_frame::DataFrame;
use rtpa_core::ieee_c37_118::random::{random_configuration_frame, random_data_frame};
use rtpa_core::ieee_c37_118::utils::{calculate_crc, now_to_hex};

use std::fs;
use std::path::Path;

#[derive(Debug, Clone)]
pub enum Protocol {
    TCP,
    UDP,
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub ip: String,
    pub port: u16,
    pub protocol: Protocol,
    pub address: String,
    pub data_rate: f64, // Hz
    pub num_pmus: Option<usize>,
    pub version: Version,
    pub use_polar: bool,
}

impl ServerConfig {
    pub fn new(
        ip: String,
        port: u16,
        protocol: Protocol,
        data_rate: f64,
        num_pmus: Option<usize>,
        version_str: &str,
        use_polar: bool,
    ) -> Result<Self, String> {
        if let Protocol::UDP = protocol {
            return Err("UDP is not implemented".to_string());
        }

        // Parse the version
        let version = match version_str {
            "2005" => Version::V2005,
            "2011" => Version::V2011,
            "2024" => Version::V2024,
            _ => return Err(format!("Unsupported version: {}", version_str)),
        };

        let address = format!("{}:{}", ip, port);
        Ok(ServerConfig {
            ip,
            port,
            protocol,
            address,
            data_rate,
            num_pmus,
            version,
            use_polar,
        })
    }
}

fn read_test_file(file_name: &str) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
    let path = Path::new("tests/test_data").join(file_name);
    let content = fs::read_to_string(path)?;

    // Filter out whitespace and convert to bytes
    let hex_string: String = content.chars().filter(|c| !c.is_whitespace()).collect();

    // Convert hex string to bytes
    let mut bytes = Vec::new();
    let mut chars = hex_string.chars();

    while let (Some(a), Some(b)) = (chars.next(), chars.next()) {
        let hex_pair = format!("{}{}", a, b);
        let byte = u8::from_str_radix(&hex_pair, 16)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        bytes.push(byte);
    }

    Ok(bytes)
}

fn update_frame_timestamp(frame: &mut Vec<u8>, time_base: u32) {
    let time_buf = now_to_hex(time_base);

    // Update SOC (seconds) - bytes 6-9
    // And FRACSEC (fractional seconds) - bytes 10-13
    frame[6..14].copy_from_slice(&time_buf);

    // Calculate and update CRC
    let crc = calculate_crc(&frame[..frame.len() - 2]);
    let frame_len = frame.len();
    frame[frame_len - 2..].copy_from_slice(&crc.to_be_bytes());
}

async fn handle_client(mut socket: tokio::net::TcpStream, config: ServerConfig) -> io::Result<()> {
    info!("MOCK PDC: Handling client");
    let mut is_streaming = false;
    let stream_interval = Duration::from_secs_f64(1.0 / config.data_rate);

    // Create configuration based on mode
    let (config_frame_bytes, mut data_frame_bytes, config_time_base) =
        if let Some(num_pmus) = config.num_pmus {
            info!("MOCK PDC: Using random mode with {} PMUs", num_pmus);
            // Create a random configuration frame
            let config_frame = random_configuration_frame(
                Some(num_pmus),
                Some(config.version),
                Some(config.use_polar),
            );
            let time_base = config_frame.time_base;

            // We'll create a new data frame each time, but we need the config to reference
            (config_frame.to_hex(), Vec::new(), time_base)
        } else {
            info!("MOCK PDC: Using fixed test files");
            // Read test files once at the start
            let config_data = read_test_file("config_message.bin")
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            let data_data = read_test_file("data_message.bin")
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

            // Extract time_base from the config frame
            let config_frame = ConfigurationFrame::from_hex(&config_data)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            let time_base = config_frame.time_base;
            (config_data, data_data, time_base)
        };

    // If we're using random mode, parse the config frame for later use with data frames
    let config_frame = if config.num_pmus.is_some() {
        match ConfigurationFrame::from_hex(&config_frame_bytes) {
            Ok(frame) => Some(frame),
            Err(e) => {
                println!("MOCK PDC: Error parsing random config frame: {}", e);
                return Err(io::Error::new(io::ErrorKind::Other, e.to_string()));
            }
        }
    } else {
        None
    };

    // Buffer for reading commands
    let mut buf = vec![0u8; 1024];
    let mut accumulated_buffer: Vec<u8> = Vec::new();
    let mut buffer_pos: usize = 0;

    loop {
        tokio::select! {
            read_result = socket.read(&mut buf) => {
                match read_result {

                    Ok(n) if n > 0 => {
                        // Append new data to accumulated buffer
                        if n < 14 {
                            info!("MOCK PDC: Warning: Read {} bytes, less than minimum 14 bytes for IEEE C37.118 frame, discarding data", n);
                            continue; // Skip appending and processing small data
                        }
                        // Append new data to accumulated buffer
                        accumulated_buffer.extend_from_slice(&buf[..n]);
                        info!("MOCK PDC: Read {} bytes, total buffer size: {}, current position: {}", n, accumulated_buffer.len(), buffer_pos);
                        // Process buffer iteratively from current position
                        while buffer_pos < accumulated_buffer.len() {
                            // Try to parse the prefix frame starting at buffer_pos
                            let remaining_data = &accumulated_buffer[buffer_pos..];
                            match PrefixFrame::from_hex(remaining_data) {
                                Ok(prefix) => {
                                    let frame_size = prefix.framesize as usize;
                                    if buffer_pos + frame_size <= accumulated_buffer.len() {
                                        // We have a complete frame, extract it
                                        let frame_data = &accumulated_buffer[buffer_pos..buffer_pos + frame_size];
                                        // Parse the frame as a command
                                        if let Ok(cmd) = CommandFrame::from_hex(frame_data) {
                                            info!("MOCK PDC: Received command: {}", cmd.command_description());

                                            // Handle the command
                                            match cmd.command_type() {
                                                Some(CommandType::SendConfigFrame2) | Some(CommandType::SendConfigFrame1) => {
                                                    // Send config frame
                                                    socket.write_all(&config_frame_bytes).await?;
                                                },
                                                Some(CommandType::TurnOnTransmission) => {
                                                    // Start data transmission
                                                    info!("MOCK PDC: Received command: Start data transmission");
                                                    is_streaming = true;
                                                },
                                                Some(CommandType::TurnOffTransmission) => {
                                                    // Stop data transmission
                                                    info!("MOCK PDC: Received command: Stop data transmission");
                                                    is_streaming = false;
                                                },
                                                Some(cmd_type) => {
                                                    info!("MOCK PDC: Received unhandled command type: {}", cmd_type);
                                                },
                                                None => {
                                                    info!("MOCK PDC: Received unknown command: {}", cmd.command);
                                                }
                                            }
                                        } else {
                                            info!("MOCK PDC: Failed to parse command frame from slice of size {}", frame_size);
                                        }
                                        // Update position to after the processed frame
                                        buffer_pos += frame_size;
                                        info!("MOCK PDC: Processed frame of size {}, new position: {}", frame_size, buffer_pos);
                                    } else {
                                        // Frame is incomplete, wait for more data
                                        info!("MOCK PDC: Incomplete frame, expected size {}, but buffer ends at {}, waiting for more data", frame_size, accumulated_buffer.len());
                                        break;
                                    }
                                },
                                Err(e) => {
                                    // Couldn't parse prefix, might be incomplete or invalid
                                    info!("MOCK PDC: Failed to parse prefix frame starting at position {}: {}. Buffer might be incomplete or invalid.", buffer_pos, e);
                                    break;
                                }
                            }
                        }

                        // If we've processed a lot and buffer_pos is far ahead, shift data to start to save memory
                        if buffer_pos > 0 && buffer_pos >= accumulated_buffer.len() / 2 {
                            if buffer_pos < accumulated_buffer.len() {
                                let remaining_data = accumulated_buffer[buffer_pos..].to_vec();
                                accumulated_buffer = remaining_data;
                            } else {
                                accumulated_buffer.clear();
                            }
                            buffer_pos = 0;
                            info!("MOCK PDC: Shifted buffer, new size: {}, new position: {}", accumulated_buffer.len(), buffer_pos);
                        }
                    },
                    Ok(0) => {
                        info!("MOCK PDC: Client disconnected");
                        break;
                    },
                    Err(e) => {
                        info!("MOCK PDC: Error reading from socket: {}", e);
                        break;
                    },
                    Ok(1_usize..)=>{
                        info!("MOCK PDC: Internal Error");
                        break;
                    }
                }
            }
            _ = time::sleep(stream_interval), if is_streaming => {
                // Different handling based on mode
                if let Some(ref cfg_frame) = config_frame {
                    // Random mode: Generate new data frame based on config
                    let data_frame = random_data_frame(cfg_frame);
                    let frame_bytes = data_frame.to_hex();

                    if let Err(e) = socket.write_all(&frame_bytes).await {
                        info!("MOCK PDC: Error sending data frame: {}", e);
                        break;
                    }
                } else {
                    // Fixed mode: Update timestamp and CRC in data frame
                    let mut frame_to_send = data_frame_bytes.clone();
                    update_frame_timestamp(&mut frame_to_send, config_time_base);

                    if let Err(e) = socket.write_all(&frame_to_send).await {
                        info!("MOCK PDC: Error sending data frame: {}", e);
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

pub async fn run_mock_server(server_config: ServerConfig) -> io::Result<()> {
    let listener = TcpListener::bind(&server_config.address).await?;
    info!("Mock PDC server listening on {}", server_config.address);
    info!(
        "Mock PDC Data rate configured to {} Hz",
        server_config.data_rate
    );

    if let Some(num_pmus) = server_config.num_pmus {
        info!("Mock PDC configured with {} random PMUs", num_pmus);
        info!("Using {} format", server_config.version);

        info!(
            "Using {} coordinates for phasors",
            if server_config.use_polar {
                "polar"
            } else {
                "rectangular"
            }
        );
    } else {
        info!("Mock PDC using fixed test data files");
    }

    while let Ok((socket, addr)) = listener.accept().await {
        info!("MOCK PDC: New client connected: {}", addr);
        let config = server_config.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_client(socket, config).await {
                info!("MOCK PDC: Client handler error: {}", e);
            }
        });
    }

    Ok(())
}
