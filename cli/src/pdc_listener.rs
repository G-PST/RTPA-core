// This file contains the implementation of the PDC listener.
//
// The PDC listener is a command line debugging tool for testing connections to the PDC.
//
// The PDC listener connects to a PDC host on a specified port with either TCP or UDP.
// It allows the user to listen on a multicast address and port.
// It tests the following items:
// - Connection establishment
// - Data transmission by logging IEEE C37.118 data and configuration messages.
// - It notifies the user if the connection is closed unexpectedly by the host or client.
// - It optionally allows the user to request the configuration frame and prints the results, by sending an IEEE C37.118 configuration frame request message.
// - It optionally allows the user to request the start of data transmission by sending an IEEE C37.118 start data transmission message.
// - It notifies the user if any messages are non-conformant to the IEEE C37.118 standard. (e.g. messages don't start with 0xAA+(frame type))
// - It reports how many data frame messages were received every N seconds.
// - It reports how many configuration frame messages were received every N seconds.
// - It notifies the user if any messages fail the CRC check.
// - Closes the connection gracefully when the user requests it.
// - Closes the connection after a timeout if no data is received.
//
// The software aims to never panic and gracefully handle errors.

use clap::{Parser, ValueEnum};
use log::{error, info, warn};
use rtpa_core::ieee_c37_118::common::FrameType;
use rtpa_core::ieee_c37_118::utils::validate_checksum;
use std::io;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream as AsyncTcpStream;
use tokio::time::timeout;

#[derive(Debug, Clone, ValueEnum)]
pub enum Protocol {
    Tcp,
    Udp,
}

#[derive(Debug, Parser)]
#[command(name = "pdc-listener")]
#[command(about = "PDC Listener for testing connections and logging messages", long_about = None)]
pub struct PdcListenerArgs {
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    #[arg(long, default_value_t = 8123)]
    pub port: u16,

    #[arg(long, default_value = "tcp")]
    pub protocol: Protocol,

    #[arg(long, default_value_t = false)]
    pub multicast: bool,

    #[arg(long, default_value_t = 10.0)]
    pub timeout: f64,

    #[arg(long, default_value_t = 5.0)]
    pub stats_interval: f64,

    #[arg(long, default_value_t = false)]
    pub request_config: bool,

    #[arg(long, default_value_t = false)]
    pub start_transmission: bool,

    #[arg(long, default_value_t = 7734)]
    pub idcode: u16,
}

pub async fn run_pdc_listener(args: PdcListenerArgs) -> io::Result<()> {
    info!("Starting PDC Listener with args: {:?}", args);

    let data_frame_count = Arc::new(AtomicUsize::new(0));
    let config_frame_count = Arc::new(AtomicUsize::new(0));
    let crc_error_count = Arc::new(AtomicUsize::new(0));
    let format_error_count = Arc::new(AtomicUsize::new(0));
    let should_stop = Arc::new(AtomicBool::new(false));

    // Spawn statistics reporting thread
    let stats_data_count = Arc::clone(&data_frame_count);
    let stats_config_count = Arc::clone(&config_frame_count);
    let stats_crc_count = Arc::clone(&crc_error_count);
    let stats_format_count = Arc::clone(&format_error_count);
    let stats_should_stop = Arc::clone(&should_stop);
    let stats_interval = args.stats_interval;
    thread::spawn(move || {
        while !stats_should_stop.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_secs_f64(stats_interval));
            let data_count = stats_data_count.swap(0, Ordering::Relaxed);
            let config_count = stats_config_count.load(Ordering::Relaxed);
            let crc_count = stats_crc_count.load(Ordering::Relaxed);
            let format_count = stats_format_count.load(Ordering::Relaxed);
            info!(
                "Stats ~ Data Frames: {}/s, Config Frames: {}, CRC Errors: {}, Format Errors: {}",
                data_count as f64 / stats_interval,
                config_count,
                crc_count,
                format_count
            );
        }
    });

    match args.protocol {
        Protocol::Tcp => {
            run_tcp_listener(
                &args.host,
                args.port,
                args.timeout,
                args.request_config,
                args.start_transmission,
                args.idcode,
                Arc::clone(&data_frame_count),
                Arc::clone(&config_frame_count),
                Arc::clone(&crc_error_count),
                Arc::clone(&format_error_count),
                Arc::clone(&should_stop),
            )
            .await
        }
        Protocol::Udp => {
            run_udp_listener(
                &args.host,
                args.port,
                args.multicast,
                args.timeout,
                args.request_config,
                args.start_transmission,
                args.idcode,
                Arc::clone(&data_frame_count),
                Arc::clone(&config_frame_count),
                Arc::clone(&crc_error_count),
                Arc::clone(&format_error_count),
                Arc::clone(&should_stop),
            )
            .await
        }
    }
}

async fn run_tcp_listener(
    host: &str,
    port: u16,
    timeout_secs: f64,
    request_config: bool,
    start_transmission: bool,
    idcode: u16,
    data_frame_count: Arc<AtomicUsize>,
    config_frame_count: Arc<AtomicUsize>,
    crc_error_count: Arc<AtomicUsize>,
    format_error_count: Arc<AtomicUsize>,
    should_stop: Arc<AtomicBool>,
) -> io::Result<()> {
    let address = format!("{}:{}", host, port);
    info!("Connecting to PDC via TCP at {}", address);

    let mut stream = match AsyncTcpStream::connect(&address).await {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to connect to {}: {}", address, e);
            return Err(e);
        }
    };
    info!("Successfully connected to {}", address);

    // Send commands if requested
    if request_config {
        let cmd_frame =
            rtpa_core::ieee_c37_118::commands::CommandFrame::new_send_config_frame2(idcode, None);
        let cmd_bytes = cmd_frame.to_hex();

        let validation_result = validate_checksum(&cmd_bytes);
        match validation_result {
            Ok(_) => {
                if let Err(e) = stream.write_all(&cmd_bytes).await {
                    error!("Failed to send config request: {}", e);
                } else {
                    info!("Sent configuration frame request: {}", cmd_frame);
                }
            }
            Err(e) => {
                error!("Failed to validate checksum: {}", e);
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid checksum",
                ));
            }
        }
    }

    if start_transmission {
        let cmd_frame =
            rtpa_core::ieee_c37_118::commands::CommandFrame::new_turn_on_transmission(idcode, None);

        let cmd_bytes = cmd_frame.to_hex();
        validate_checksum(&cmd_bytes).unwrap();

        if let Err(e) = stream.write_all(&cmd_bytes).await {
            error!("Failed to send start transmission command: {}", e);
        } else {
            info!("Sent start transmission command: {}", cmd_frame);
        }
    }

    let mut buffer = vec![0; 65535];
    let timeout_duration = Duration::from_secs_f64(timeout_secs);
    let mut last_data_time = Instant::now();

    loop {
        if should_stop.load(Ordering::Relaxed) {
            info!("Shutting down TCP listener");
            break;
        }

        match timeout(timeout_duration, stream.read(&mut buffer)).await {
            Ok(Ok(bytes_read)) if bytes_read > 0 => {
                last_data_time = Instant::now();
                process_buffer(
                    &buffer[..bytes_read],
                    &data_frame_count,
                    &config_frame_count,
                    &crc_error_count,
                    &format_error_count,
                );
            }
            Ok(Ok(_)) => {
                warn!("Connection closed by remote host");
                break;
            }
            Ok(Err(e)) => {
                error!("Read error: {}", e);
                break;
            }
            Err(_) => {
                if last_data_time.elapsed() > timeout_duration {
                    error!("Timeout: No data received for {} seconds", timeout_secs);
                    break;
                }
            }
        }
    }

    should_stop.store(true, Ordering::Relaxed);
    Ok(())
}

async fn run_udp_listener(
    host: &str,
    port: u16,
    multicast: bool,
    timeout_secs: f64,
    request_config: bool,
    start_transmission: bool,
    idcode: u16,
    data_frame_count: Arc<AtomicUsize>,
    config_frame_count: Arc<AtomicUsize>,
    crc_error_count: Arc<AtomicUsize>,
    format_error_count: Arc<AtomicUsize>,
    should_stop: Arc<AtomicBool>,
) -> io::Result<()> {
    let address = format!("{}:{}", host, port);
    info!("Binding to PDC via UDP at {}", address);

    // For UDP, we need to bind to the address
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    if multicast {
        // Join multicast group if requested
        info!("Joining multicast group {}", host);
        socket.join_multicast_v4(&host.parse().unwrap(), &"0.0.0.0".parse().unwrap())?;
    }

    socket.connect(&address)?;
    info!("Connected to {}", address);

    // Send commands if requested
    if request_config {
        let cmd_frame =
            rtpa_core::ieee_c37_118::commands::CommandFrame::new_send_config_frame2(idcode, None);
        let cmd_bytes = cmd_frame.to_hex();
        if let Err(e) = socket.send(&cmd_bytes) {
            error!("Failed to send config request: {}", e);
        } else {
            info!("Sent configuration frame request");
        }
    }

    if start_transmission {
        let cmd_frame =
            rtpa_core::ieee_c37_118::commands::CommandFrame::new_turn_on_transmission(idcode, None);
        let cmd_bytes = cmd_frame.to_hex();
        if let Err(e) = socket.send(&cmd_bytes) {
            error!("Failed to send start transmission command: {}", e);
        } else {
            info!("Sent start transmission command");
        }
    }

    let mut buffer = vec![0; 65535];
    let timeout_duration = Duration::from_secs_f64(timeout_secs);
    let mut last_data_time = Instant::now();

    loop {
        if should_stop.load(Ordering::Relaxed) {
            info!("Shutting down UDP listener");
            break;
        }

        match socket.recv_from(&mut buffer) {
            Ok((bytes_read, _addr)) if bytes_read > 0 => {
                last_data_time = Instant::now();
                process_buffer(
                    &buffer[..bytes_read],
                    &data_frame_count,
                    &config_frame_count,
                    &crc_error_count,
                    &format_error_count,
                );
            }
            Ok(_) => {
                warn!("Received empty packet");
            }
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut =>
            {
                if last_data_time.elapsed() > timeout_duration {
                    error!("Timeout: No data received for {} seconds", timeout_secs);
                    break;
                }
            }
            Err(e) => {
                error!("UDP receive error: {}", e);
                break;
            }
        }
    }

    should_stop.store(true, Ordering::Relaxed);
    Ok(())
}

fn process_buffer(
    buffer: &[u8],
    data_frame_count: &AtomicUsize,
    config_frame_count: &AtomicUsize,
    crc_error_count: &AtomicUsize,
    format_error_count: &AtomicUsize,
) {
    // Check if message starts with 0xAA
    if buffer.len() < 2 || buffer[0] != 0xAA {
        format_error_count.fetch_add(1, Ordering::Relaxed);
        warn!("Invalid message: Does not start with 0xAA sync byte");
        return;
    }

    // Attempt to parse only the prefix frame to determine frame type
    match rtpa_core::ieee_c37_118::common::PrefixFrame::from_hex(&buffer) {
        Ok(prefix) => {
            // Validate CRC for the entire frame
            if let Err(_) = rtpa_core::ieee_c37_118::utils::validate_checksum(buffer) {
                crc_error_count.fetch_add(1, Ordering::Relaxed);
                warn!("CRC checksum failed for received frame");
                return;
            }

            match rtpa_core::ieee_c37_118::common::FrameType::from_sync(prefix.sync) {
                Ok(frame_type) => match frame_type {
                    FrameType::Data => {
                        data_frame_count.fetch_add(1, Ordering::Relaxed);

                        if data_frame_count.load(Ordering::Relaxed) == 1 {
                            info!("Received Data Frame (ID: {})", prefix.idcode);
                        }
                    }
                    FrameType::Header => {
                        info!("Received Header Frame (ID: {})", prefix.idcode);
                    }
                    FrameType::Config1 | FrameType::Config2 | FrameType::Config3 => {
                        config_frame_count.fetch_add(1, Ordering::Relaxed);
                        info!("Received Configuration Frame (ID: {})", prefix.idcode);
                    }
                    FrameType::Command => {
                        info!("Received Command Frame (ID: {})", prefix.idcode);
                    }
                },
                Err(e) => {
                    format_error_count.fetch_add(1, Ordering::Relaxed);
                    warn!("Invalid frame type: {:?}", e);
                }
            }
        }
        Err(e) => {
            format_error_count.fetch_add(1, Ordering::Relaxed);
            warn!("Failed to parse prefix frame: {:?}", e);
        }
    }
}
