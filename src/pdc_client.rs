// This module is for connecting to a pdc server,
// getting the configuration frame and header frames.
// starting a data stream and storing the results in a ring buffer
// stopping the data stream and shutting down.
//
// It uses a background thread to constantly read from the PDC server,
// allowing the main thread to grab copies of the buffer when needed.
#![allow(unused)]
use crate::{
    frame_parser::parse_config_frame_1and2,
    frames::{calculate_crc, CommandFrame2011, ConfigurationFrame1and2_2011, PrefixFrame2011},
};
use bytes::BytesMut;
use std::collections::VecDeque;
use std::error::Error;
use std::io;
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc; // For efficient byte management
use tracing::{debug, error, info, warn};

// Define an enum to represent different buffer types
enum BufferType {
    Stack([u8; 30 * 1024]),                // 30KB stack buffer
    Heap(VecDeque<(SystemTime, Vec<u8>)>), // Heap buffer with timestamps
}

pub enum ControlMessage {
    Stop,
    GetBuffer,
    //GetBufferDuration(Duration),
}

pub struct PDCClient {
    stream: TcpStream,
    //allocate 30 kB to the stack to serve as a ring buffer.
    buffer: BufferType,
    duration: Duration,
    max_buffer_size: usize, // The max size of the buffer we
    // can fit whole frames = frame_size * N <= 30kB
    pub frame_size: usize,   // Size of each individual dataframe
    pub write_offset: usize, // location of the next slot for incoming data.
    pub idcode: u16,
    control_tx: mpsc::Sender<ControlMessage>,
    control_rx: mpsc::Receiver<ControlMessage>,
    data_tx: mpsc::Sender<Vec<u8>>,
    pub config: Option<ConfigurationFrame1and2_2011>,
}

impl PDCClient {
    // TODO update to allow creation of pdc client with host, port and idcode.
    pub async fn new(
        host: &str,
        port: u16,
        idcode: u16,
        duration: Duration,
    ) -> Result<
        (Self, mpsc::Sender<ControlMessage>, mpsc::Receiver<Vec<u8>>),
        Box<dyn Error + Send + Sync>,
    > {
        info!("Attempting to connect to {}:{}", host, port);
        let addr = format!("{}:{}", host, port);

        //let stream = tokio::net::TcpStream::connect(&addr).await?;
        let stream = tokio::net::TcpStream::connect(&addr).await.map_err(|e| {
            error!("Failed to connect to PDC server: {}", e);
            io::Error::new(io::ErrorKind::ConnectionRefused, e)
        })?;

        info!("Successfully connected to PDC server");
        let (control_tx, control_rx) = mpsc::channel(32);
        let (data_tx, data_rx) = mpsc::channel(1024 * 30);

        // calc max buffer initially
        const STACK_SIZE: usize = 30 * 1024;
        let frame_size = 52; // Updated after getting config frame.
        let max_buffer_size = STACK_SIZE - (STACK_SIZE % frame_size);
        let mut client = PDCClient {
            stream,
            buffer: BufferType::Stack([0; 30 * 1024]),
            duration,
            max_buffer_size,
            frame_size: 0,
            write_offset: 0,
            idcode,
            control_tx: control_tx.clone(),
            control_rx,
            data_tx,
            config: None,
        };

        // Get initial configuration
        info!("Getting configuration");
        let config = client.get_config_frame().await.unwrap();
        client.config = Some(config);
        info!("Got Configuration: {} PMUs", 1);
        client.initialize_buffer()?;

        Ok((client, control_tx, data_rx))
    }
    // Add a reconnect method that only updates the TCP connection
    pub async fn reconnect(
        &mut self,
        host: &str,
        port: u16,
        idcode: u16,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Reconnect TCP stream
        self.stream = TcpStream::connect((host, port)).await?;

        // Reinitialize connection
        self.start_stream();
        //self.initialize_connection(idcode).await?;

        Ok(())
    }

    fn calculate_frame_size(&self) -> usize {
        // Calculate frame size based on configuration
        // This will depend on your specific PMU configuration
        let frame_size = if let Some(ref config) = self.config {
            // Start with the common frame size (prefix + chk)
            let mut size = 14 + 2;

            for pmu_config in &config.pmu_configs {
                // Add size for STAT word
                size += 2;

                // Add size for phasors (format determines whether fixed or float)
                size += pmu_config.phasor_size() * pmu_config.phnmr as usize;

                // Add size for FREQ and DFREQ
                size += 2 * pmu_config.freq_dfreq_size();

                // Add size for analog values
                size += pmu_config.analog_size() * pmu_config.annmr as usize;

                // Add size for digital words
                size += 2 * pmu_config.dgnmr as usize;
            }
            size
        } else {
            0
        };

        frame_size
    }

    fn initialize_buffer(&mut self) -> Result<(), std::io::Error> {
        if let Some(ref config) = self.config {
            // Calculate expected frame size based on configuration
            let frame_size = self.calculate_frame_size();
            self.frame_size = frame_size;

            // Calculate required buffer size based on data rate and buffer duration
            let data_rate = config.data_rate as f64;
            let frames_per_second = data_rate.abs();
            let total_frames = (frames_per_second as u64 * self.duration.as_secs()) as usize;
            self.max_buffer_size = frame_size * total_frames;

            // Switch to heap buffer if required size is too large
            if self.max_buffer_size > 30 * 1024 {
                self.buffer = BufferType::Heap(VecDeque::with_capacity(total_frames));
                info!(
                    "Using heap buffer with capacity for {} frames",
                    total_frames
                );
            } else {
                info!("Using stack buffer");
            }
        }
        Ok(())
    }
    pub fn get_config(&mut self) -> Option<ConfigurationFrame1and2_2011> {
        return self.config.clone();
    }

    pub async fn get_config_frame(&mut self) -> io::Result<ConfigurationFrame1and2_2011> {
        info!("Requesting configuration frame...");

        // Create command frame for config request
        let cmd_frame = CommandFrame2011::new_send_config_frame1(self.idcode);
        let cmd_bytes = cmd_frame.to_hex();

        // Send command
        info!("Sending config request command...");
        self.stream.write_all(&cmd_bytes).await?;
        info!("Config request command sent");

        // Read response
        // First read common header (14 bytes) to get frame size
        let mut header_buf = [0u8; 14];
        self.stream.read_exact(&mut header_buf).await?;

        if let Ok(prefix) = PrefixFrame2011::from_hex(&header_buf) {
            // Read the rest of the frame
            let remaining_size = prefix.framesize as usize - 14;

            info!("Reading rest of frame bytes:{}", remaining_size);
            let mut config_buf = BytesMut::with_capacity(remaining_size);

            self.stream.try_read_buf(&mut config_buf);
            let mut complete_frame = Vec::with_capacity(prefix.framesize as usize);
            complete_frame.extend_from_slice(&header_buf);
            complete_frame.extend_from_slice(&config_buf);

            // Verify CRC
            let calculated_crc = calculate_crc(&complete_frame[..complete_frame.len() - 2]);
            let frame_crc = u16::from_be_bytes([
                complete_frame[prefix.framesize as usize - 2],
                complete_frame[prefix.framesize as usize - 1],
            ]);

            if calculated_crc != frame_crc {
                warn!(
                    "CRC mismatch: calculated={:04x}, received={:04x}",
                    calculated_crc, frame_crc
                );
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "CRC check failed",
                ));
            }

            // Parse configuration frame
            match parse_config_frame_1and2(&complete_frame) {
                Ok(config) => {
                    info!("Successfully parsed configuration frame");
                    // Update frame size based on configuration
                    self.frame_size = config.calc_data_frame_size();
                    self.max_buffer_size = 30 * 1024 - ((30 * 1024) % self.frame_size);
                    info!(
                        "Updated frame_size to {} and max_buffer_size to {}",
                        self.frame_size, self.max_buffer_size
                    );
                    Ok(config)
                }
                Err(_) => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Failed to parse configuration frame",
                )),
            }
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Failed to parse frame prefix",
            ))
        }
    }

    async fn read_frame(&mut self) -> io::Result<Option<Vec<u8>>> {
        let mut buf = vec![0u8; self.frame_size];

        match tokio::time::timeout(Duration::from_secs(1), self.stream.read(&mut buf)).await {
            Ok(Ok(n)) => {
                if n == 0 {
                    error!("Connection closed by server");
                    self.shutdown().await;
                    return Err(io::Error::new(
                        io::ErrorKind::ConnectionAborted,
                        "Server closed connection",
                    ));
                    //return Ok(None);
                }
                if n == self.frame_size {
                    Ok(Some(buf))
                } else {
                    warn!("Partial read: {} bytes of expected {}", n, self.frame_size);
                    // You might want to handle partial reads differently
                    Ok(None)
                }
            }
            Ok(Err(e)) => {
                error!("Error reading from stream: {}", e);
                Err(e)
            }
            Err(_) => {
                error!("Timeout reading frame");
                Ok(None)
            }
        }
    }

    pub async fn start_stream(&mut self) {
        info!("PDC client stream starting...");
        let control_tx = self.control_tx.clone();

        // Send command to start data transmission
        info!("Sending command to start data transmission...");
        let cmd_frame = CommandFrame2011::new_turn_on_transmission(self.idcode);
        if let Err(e) = self.stream.write_all(&cmd_frame.to_hex()).await {
            error!("Failed to send start transmission command: {}", e);
            self.shutdown().await;
            return;
        }

        let data_tx = self.data_tx.clone();
        let mut control_rx = std::mem::replace(&mut self.control_rx, mpsc::channel(32).1);

        let mut consecutive_errors = 0;
        const MAX_CONSECUTIVE_ERRORS: u32 = 10;
        loop {
            tokio::select! {
                // Use async recv() instead of try_recv()
                Some(control_msg) = control_rx.recv() => {
                    match control_msg {
                        ControlMessage::Stop => {
                            info!("PDC client received Stop command");
                            break;
                        },
                        ControlMessage::GetBuffer => {
                            println!("PDC client received GetBuffer command");
                            match &self.buffer {
                                BufferType::Stack(buf) => {
                                    info!("Sending buffer data of size {}", self.max_buffer_size);
                                    if let Err(e) = data_tx.send(buf[..self.max_buffer_size].to_vec()).await {
                                        println!("Failed to send buffer data: {}", e);
                                    }
                                }
                                BufferType::Heap(_) => {
                                    let result = self.get_buffer_contents();
                                    if let Err(e) = data_tx.send(result).await {
                                        info!("Failed to send heap buffer data: {}", e);
                                    }
                                    //println!("Heap buffer not implemented yet");
                                }
                            }
                        }
                    }
                },
                result = self.read_frame() => {
                    match result {
                        Ok(Some(frame)) => {
                            consecutive_errors = 0; //reset error cnt
                            //println!("PDC client received frame of size {}", frame.len());
                            self.store_frame(&frame);
                        }
                        Ok(None) => {
                            consecutive_errors += 1;
                            info!("No frame available");
                        }
                        Err(e) => {
                            error!("Error reading frame: {}", e);
                            consecutive_errors +=1 ;
                        }
                    }
                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS{
                        error!("Too many consecutive errors, shutting down");
                        break;
                    }
                }
            }
        }
        self.shutdown().await;
        self.control_rx = control_rx;
        println!("PDC client stream ending...");
    }

    pub fn get_control_sender(&self) -> mpsc::Sender<ControlMessage> {
        self.control_tx.clone()
    }

    // End of Receive loop.
    //
    fn store_frame(&mut self, frame_data: &[u8]) {
        //println!("Storing data frame");
        match &mut self.buffer {
            BufferType::Stack(buffer) => {
                // Check if frame fits at current offset
                if self.write_offset + self.frame_size > self.max_buffer_size {
                    // Not enough space at end, start from beginning
                    self.write_offset = 0;
                }

                // Copy frame data to buffer
                buffer[self.write_offset..self.write_offset + self.frame_size]
                    .copy_from_slice(frame_data);

                // Update write offset
                self.write_offset += self.frame_size;
            }
            BufferType::Heap(buffer) => {
                let now = SystemTime::now();
                buffer.push_back((now, frame_data.to_vec()));

                // Remove old frames based on buffer duration
                while let Some((timestamp, _)) = buffer.front() {
                    if now.duration_since(*timestamp).unwrap() > self.duration {
                        buffer.pop_front();
                    } else {
                        break;
                    }
                }
            }
        }
    }
    pub fn get_buffer_contents(&self) -> Vec<u8> {
        match &self.buffer {
            BufferType::Stack(buffer) => {
                // Return slice up to write_offset
                println!("Getting buffer from stack");
                buffer[..self.max_buffer_size].to_vec()
            }
            BufferType::Heap(buffer) => {
                // Concatenate all frames in the heap buffer into a single Vec<u8>
                println!("getting buffer from heap");
                let mut result = Vec::new();
                for (_, frame) in buffer {
                    result.extend_from_slice(frame);
                }
                result
            }
        }
    }

    pub fn get_frame_size(&self) -> usize {
        self.frame_size
    }
    // Handle Shutdown
    async fn shutdown(&mut self) {
        println!("Shutting down PDC client...");

        // Send stop command to PDC server
        let cmd_frame = CommandFrame2011::new_turn_off_transmission(self.idcode);
        if let Err(e) = self.stream.write_all(&cmd_frame.to_hex()).await {
            println!("Failed to send stop transmission command: {}", e);
        }

        // Close the stream
        if let Err(e) = self.stream.shutdown().await {
            println!("Error shutting down stream: {}", e);
        }

        // Clear the buffer
        match &mut self.buffer {
            BufferType::Stack(buf) => buf.fill(0),
            BufferType::Heap(buf) => buf.clear(),
        }
    }
}
