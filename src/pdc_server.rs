#![allow(unused)]
//const BUFFER_MAX_SIZE: u32 = 104857600;
//const SECONDS_PER_MINUTE: u32 = 60;
//use rand::rngs::StdRng;
//use rand::{Rng, SeedableRng};
//use serde::de::Error;
use crate::frames::calculate_crc;
use std::error::Error;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::{self, Duration};

#[derive(Debug, Clone)]
pub enum Protocol {
    TCP,
    UDP,
}

use crate::frame_parser::{parse_frame, Frame};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub ip: String,
    pub port: u16,
    pub protocol: Protocol,
    pub address: String,
    pub data_rate: f64, // Hz
}

impl ServerConfig {
    pub fn new(ip: String, port: u16, protocol: Protocol, data_rate: f64) -> Result<Self, String> {
        if let Protocol::UDP = protocol {
            return Err("UDP is not implemented".to_string());
        }
        let address = format!("{}:{}", ip, port);
        Ok(ServerConfig {
            ip,
            port,
            protocol,
            address,
            data_rate,
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
fn update_frame_timestamp(frame: &mut Vec<u8>) {
    // Get current time
    let now = std::time::SystemTime::now();
    let duration = now
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(std::time::Duration::from_secs(0));

    // Extract seconds and fractional seconds
    let secs = duration.as_secs() as u32;
    let nanos = duration.subsec_nanos();
    let fracsec = (nanos as f64 / 1_000_000_000.0 * 16777216.0) as u32; // Convert to 24-bit fraction

    // Update SOC (seconds) - bytes 6-9
    frame[6..10].copy_from_slice(&secs.to_be_bytes());

    // Update FRACSEC - bytes 10-13
    frame[10..14].copy_from_slice(&fracsec.to_be_bytes());

    // Calculate and update CRC
    let crc = calculate_crc(&frame[..frame.len() - 2]);
    let frame_len = frame.len();
    frame[frame_len - 2..].copy_from_slice(&crc.to_be_bytes());
}

async fn handle_client(mut socket: tokio::net::TcpStream, config: ServerConfig) -> io::Result<()> {
    println!("Handling client");
    let mut is_streaming = false;
    let stream_interval = Duration::from_secs_f64(1.0 / config.data_rate);

    // Read test files once at the start
    let config_frame = read_test_file("config_message.bin")
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
    let mut data_frame = read_test_file("data_message.bin")
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

    // Buffer for reading commands
    let mut buf = vec![0u8; 1024];

    loop {
        tokio::select! {
            read_result = socket.read(&mut buf) => {
                match read_result {
                    Ok(n) if n > 0 => {

                        // Parse command frame
                        if let Ok(frame) = parse_frame(&buf[..n], None) {
                            match frame {
                                Frame::Command(cmd) => {
                                    match cmd.command {
                                        4 => { // Send config frame
                                            println!("Received command: Send configuration frame");
                                            match read_test_file("config_message.bin") {
                                                Ok(config_data) => {
                                                    socket.write_all(&config_data).await?;
                                                },
                                                Err(e) => {
                                                    println!("Error reading config file: {}", e);
                                                }
                                            }
                                        },
                                        2 => { // Start data transmission
                                            println!("Received command: Start data transmission");
                                            is_streaming = true;
                                        },
                                        1 => { // Stop data transmission
                                            println!("Received command: Stop data transmission");
                                            is_streaming = false;
                                        },
                                        _ => {
                                            println!("Received unknown command: {}", cmd.command);
                                        }
                                    }
                                },
                                _ => println!("Received non-command frame"),
                            }
                        }
                    },
                    Ok(0) => {
                        println!("Client disconnected");
                        break;
                    },
                    Err(e) => {
                        println!("Error reading from socket: {}", e);
                        break;
                    },
                    Ok(1_usize..)=>{
                        println!("Internal Error");
                        break;
                    }
                }
            }
            _ = time::sleep(stream_interval), if is_streaming => {
                // Update timestamp and CRC in data frame
                let mut frame_to_send = data_frame.clone();
                update_frame_timestamp(&mut frame_to_send);

                if let Err(e) = socket.write_all(&frame_to_send).await {
                    println!("Error sending data frame: {}", e);
                    break;
                }
            }
        }
    }

    Ok(())
}

pub async fn run_mock_server(server_config: ServerConfig) -> io::Result<()> {
    let listener = TcpListener::bind(&server_config.address).await?;
    println!("Mock PDC server listening on {}", server_config.address);
    println!("Data rate configured to {} Hz", server_config.data_rate);

    while let Ok((socket, addr)) = listener.accept().await {
        println!("New client connected: {}", addr);
        let config = server_config.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_client(socket, config).await {
                println!("Client handler error: {}", e);
            }
        });
    }

    Ok(())
}
