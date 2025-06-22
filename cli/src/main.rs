mod arrow_utils;
mod frame_parser;
mod frames;
mod pdc_buffer_server;
mod pdc_client;
mod pdc_listener;
mod pdc_server;
use clap::{Parser, Subcommand};
use log::info;
use pdc_server::{run_mock_server, Protocol, ServerConfig};
use tokio::io;

#[derive(Debug, Parser)] // requires `derive` feature
#[command(name = "pmu")]
#[command(about = "Testing PMU", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    MockPDC {
        #[arg(long, default_value = "127.0.0.1")]
        ip: String,

        #[arg(long, default_value_t = 8123)]
        port: u16,

        #[arg(long, default_value_t = 30.0)]
        data_rate: f64,

        #[arg(long)]
        num_pmus: Option<usize>,

        #[arg(long, default_value = "2005")]
        version: String,

        #[arg(long, default_value_t = false)]
        polar: bool,
    },
    //#[command(arg_required_else_help = true)]
    Server {
        #[arg(long, default_value = "127.0.0.1")]
        pdc_ip: String,
        #[arg(long, default_value_t = 8123)]
        pdc_port: u16,
        #[arg(long, default_value_t = 8080)]
        pdc_idcode: u16,
        #[arg(long, default_value_t = 7734)]
        http_port: u16,
        #[arg(long, default_value_t = 120)]
        duration: u16,
    },
    PdcListener {
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        #[arg(long, default_value_t = 8123)]
        port: u16,
        #[arg(long, default_value = "tcp")]
        protocol: String,
        #[arg(long, default_value_t = false)]
        multicast: bool,
        #[arg(long, default_value_t = 10.0)]
        timeout: f64,
        #[arg(long, default_value_t = 5.0)]
        stats_interval: f64,
        #[arg(long, default_value_t = false)]
        request_config: bool,
        #[arg(long, default_value_t = false)]
        start_transmission: bool,
        #[arg(long, default_value_t = 7734)]
        idcode: u16,
    },
}

#[tokio::main]
async fn main() -> io::Result<()> {
    // Initialize logging early to ensure all log messages are captured
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    println!("Starting application");
    log::info!("Logging initialized for PMU application");

    let args = Cli::parse();

    match args.command {
        Commands::MockPDC {
            ip,
            port,
            data_rate,
            num_pmus,
            version,
            polar,
        } => {
            println!("Using {ip} and port {port}");

            // Configuration mode message
            if num_pmus.is_some() {
                println!("Running in random PMU mode with {} PMUs", num_pmus.unwrap());
            } else {
                println!("Running in fixed test data mode");
            }

            let server_config = ServerConfig::new(
                ip,
                port,
                Protocol::TCP,
                data_rate,
                num_pmus,
                &version,
                polar,
            )
            .unwrap();

            run_mock_server(server_config)
                .await
                .expect("Server failed to start");
        }
        Commands::Server {
            pdc_ip,
            pdc_port,
            pdc_idcode,
            http_port,
            duration,
        } => {
            // Start the pdc buffer server
            std::env::set_var("PDC_HOST", &pdc_ip);
            std::env::set_var("PDC_PORT", &pdc_port.to_string());
            std::env::set_var("PDC_IDCODE", &pdc_idcode.to_string());
            std::env::set_var("SERVER_PORT", &http_port.to_string());
            std::env::set_var("BUFFER_DURATION_SECS", &duration.to_string());

            let buffer_server_handle = tokio::spawn(async move {
                if let Err(e) = pdc_buffer_server::run().await {
                    println!("Buffer server error: {}", e);
                }
            });
            // Keep main thread running
            tokio::signal::ctrl_c()
                .await
                .expect("Failed to listen for ctrl+c signal");
            info!("Shutting down...");
            buffer_server_handle.abort();
        }
        Commands::PdcListener {
            host,
            port,
            protocol,
            multicast,
            timeout,
            stats_interval,
            request_config,
            start_transmission,
            idcode,
        } => {
            let listener_args = pdc_listener::PdcListenerArgs {
                host,
                port,
                protocol: match protocol.to_lowercase().as_str() {
                    "udp" => pdc_listener::Protocol::Udp,
                    _ => pdc_listener::Protocol::Tcp,
                },
                multicast,
                timeout,
                stats_interval,
                request_config,
                start_transmission,
                idcode,
            };
            println!("Starting PDC Listener with arguments: {:?}", listener_args);
            if let Err(e) = pdc_listener::run_pdc_listener(listener_args).await {
                println!("PDC listener error: {}", e);
                log::error!("PDC listener error: {}", e);
            }
        }
    }
    Ok(())
}
