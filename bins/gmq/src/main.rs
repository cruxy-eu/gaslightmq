use clap::{Args, Parser, Subcommand};
use gaslightmq::proxy_api::RuleSet;
use gaslightmq::{NodePorts, Proxy, Scenario};
use std::net::SocketAddr;
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
    /// Proxy a real Kafka cluster and inject faults, controlled from a web UI
    Proxy(ProxyArgs),
}

#[derive(Args)]
struct ProxyArgs {
    /// Bootstrap address of the upstream cluster
    #[arg(short, long, value_name = "HOST:PORT")]
    upstream: String,

    /// Address clients bootstrap from
    #[arg(short, long, default_value = "0.0.0.0:19092")]
    bind: SocketAddr,

    /// First port for per-broker listeners [default: bind port + 1]
    #[arg(long, value_name = "PORT")]
    node_port_base: Option<u16>,

    /// Host advertised to clients [default: bind IP, or 127.0.0.1 for 0.0.0.0]
    #[arg(long, value_name = "HOST")]
    advertise_host: Option<String>,

    /// Address of the web UI and control API
    #[arg(short, long, default_value = "0.0.0.0:9000")]
    control: SocketAddr,

    /// YAML file with fault rows active from the start
    #[arg(short, long, value_name = "FILE")]
    rules: Option<PathBuf>,

    /// Seed for fault probabilities, for reproducible runs
    #[arg(long)]
    seed: Option<u64>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Some(Command::Health) => {
            let status = gaslightmq::health::check().await;
            eprintln!("{status}");
            process::exit(if status.is_healthy() { 0 } else { 1 });
        }
        Some(Command::Proxy(args)) => run_proxy(args).await,
        None => run_broker(cli.scenario, &cli.bind).await,
    }
}

async fn run_broker(scenario: Option<PathBuf>, bind: &str) -> anyhow::Result<()> {
    let broker = if let Some(path) = scenario {
        let yaml = std::fs::read_to_string(&path)?;
        let scenario = Scenario::from_yaml(&yaml)?;
        gaslightmq::BrokerBuilder::from(scenario).bind(bind).start().await?
    } else {
        gaslightmq::BrokerBuilder::new().bind(bind).start().await?
    };

    gaslightmq::health::write_addr(broker.addr());

    eprintln!("Gaslight MQ listening on {}", broker.addr());
    eprintln!("Press Ctrl+C to stop");

    tokio::signal::ctrl_c().await?;
    broker.shutdown().await;
    Ok(())
}

async fn run_proxy(args: ProxyArgs) -> anyhow::Result<()> {
    gaslightmq_logging::init();

    let rules = match &args.rules {
        Some(path) => serde_yaml::from_str::<RuleSet>(&std::fs::read_to_string(path)?)?,
        None => RuleSet::default(),
    };
    let base = args.node_port_base.unwrap_or_else(|| args.bind.port().saturating_add(1));
    let mut builder = Proxy::builder()
        .upstream(args.upstream)
        .bind(args.bind)
        .node_ports(NodePorts::Sequential(base))
        .control(args.control)
        .rules(rules);
    if let Some(host) = args.advertise_host {
        builder = builder.advertise_host(host);
    }
    if let Some(seed) = args.seed {
        builder = builder.seed(seed);
    }

    let proxy = builder.start().await?;
    gaslightmq::health::write_addr(proxy.bootstrap_addr());

    eprintln!("Gaslight MQ proxy: clients bootstrap from {}", proxy.bootstrap_addr());
    if let Some(control) = proxy.control_addr() {
        eprintln!("Control surface on http://{control}");
    }
    for node in proxy.handle().nodes() {
        eprintln!("  node {} {} -> :{}", node.node_id, node.upstream, node.proxy_port);
    }
    eprintln!("Press Ctrl+C to stop");

    tokio::signal::ctrl_c().await?;
    proxy.shutdown().await;
    Ok(())
}
