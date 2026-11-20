use std::fmt;
use std::path::PathBuf;
use tokio::net::TcpStream;

const ADDR_FILENAME: &str = "gmq.addr";
const DEFAULT_ADDR: &str = "127.0.0.1:9092";

fn addr_file() -> PathBuf {
    std::env::temp_dir().join(ADDR_FILENAME)
}

pub enum HealthStatus {
    Healthy(String),
    Unhealthy(String),
}

impl fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Healthy(addr) => write!(f, "healthy: {addr}"),
            Self::Unhealthy(addr) => write!(f, "unhealthy: {addr}"),
        }
    }
}

impl HealthStatus {
    pub fn is_healthy(&self) -> bool {
        matches!(self, Self::Healthy(_))
    }
}

fn resolve_addr() -> String {
    let path = addr_file();
    match std::fs::read_to_string(&path) {
        Ok(s) => {
            let trimmed = s.trim().to_string();
            if trimmed.is_empty() {
                eprintln!("warning: {} is empty, falling back to {DEFAULT_ADDR}", path.display());
                DEFAULT_ADDR.to_string()
            } else {
                trimmed
            }
        }
        Err(e) => {
            eprintln!("warning: failed to read {}: {e}, falling back to {DEFAULT_ADDR}", path.display());
            DEFAULT_ADDR.to_string()
        }
    }
}

pub fn write_addr(addr: std::net::SocketAddr) {
    let path = addr_file();
    let health_addr = addr.to_string().replace("0.0.0.0", "127.0.0.1");
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    if let Err(e) = std::fs::write(&path, &health_addr) {
        eprintln!("warning: failed to write {}: {e}, health check will use default address", path.display());
    }
}

pub async fn check() -> HealthStatus {
    let addr = resolve_addr();
    match TcpStream::connect(&addr).await {
        Ok(_) => HealthStatus::Healthy(addr),
        Err(_) => HealthStatus::Unhealthy(addr),
    }
}
