mod arrow_utils;
mod frame_parser;
mod frames;
mod pdc_buffer_server;
mod pdc_client;
mod pdc_server;
use clap::{Parser, Subcommand};
//use log::info;
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
}

#[tokio::main]
async fn main() -> io::Result<()> {
    //env_logger::init();
    //info!("Starting application");

    let args = Cli::parse();

    match args.command {
        Commands::MockPDC { ip, port } => {
            println!("Using {ip} and port {port}");
            let server_config = ServerConfig::new(ip, port, Protocol::TCP, 30.0).unwrap();

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
            println!("Shutting down...");
            buffer_server_handle.abort();
        }
    }
    Ok(())
}
