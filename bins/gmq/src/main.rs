use clap::{Parser, Subcommand};
use gaslightmq::Scenario;
use std::path::PathBuf;
use std::process;

#[derive(Parser)]
#[command(name = "gmq", about = "Gaslight MQ — a fake Kafka broker for chaos testing")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    #[arg(short, long, help = "Path to YAML scenario file")]
    scenario: Option<PathBuf>,

    #[arg(short, long, default_value = "0.0.0.0:9092", help = "Address to bind to")]
    bind: String,
}

#[derive(Subcommand)]
enum Command {
    /// Check if the broker is healthy by attempting a TCP connection
    Health,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    if let Some(Command::Health) = cli.command {
        let status = gaslightmq::health::check().await;
        eprintln!("{status}");
        process::exit(if status.is_healthy() { 0 } else { 1 });
    }

    let broker = if let Some(path) = cli.scenario {
        let yaml = std::fs::read_to_string(&path)?;
        let scenario = Scenario::from_yaml(&yaml)?;
        gaslightmq::BrokerBuilder::from(scenario)
            .bind(&cli.bind)
            .start()
            .await?
    } else {
        gaslightmq::BrokerBuilder::new()
            .bind(&cli.bind)
            .start()
            .await?
    };

    gaslightmq::health::write_addr(broker.addr());

    eprintln!("Gaslight MQ listening on {}", broker.addr());
    eprintln!("Press Ctrl+C to stop");

    tokio::signal::ctrl_c().await?;
    broker.shutdown().await;

    Ok(())
}
