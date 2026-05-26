use clap::Parser;
use gaslightmq::Scenario;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "gmq", about = "Gaslight MQ — a fake Kafka broker for chaos testing")]
struct Cli {
    #[arg(short, long, help = "Path to YAML scenario file")]
    scenario: Option<PathBuf>,

    #[arg(short, long, default_value = "0.0.0.0:9092", help = "Address to bind to")]
    bind: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let broker = if let Some(path) = cli.scenario {
        let yaml = std::fs::read_to_string(&path)?;
        let scenario = Scenario::from_yaml(&yaml)?;
        gaslightmq::Broker::start(gaslightmq::into_broker_config(scenario, &cli.bind)).await?
    } else {
        gaslightmq::BrokerBuilder::new()
            .topic("default", gaslightmq::BlackHole::new(1))
            .bind(&cli.bind)
            .start()
            .await?
    };

    eprintln!("Gaslight MQ listening on {}", broker.addr());
    eprintln!("Press Ctrl+C to stop");

    tokio::signal::ctrl_c().await?;
    broker.shutdown().await;

    Ok(())
}
