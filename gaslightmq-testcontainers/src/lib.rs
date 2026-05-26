use std::path::{Path, PathBuf};

use gaslightmq_scenario::Scenario;
use testcontainers::core::{ContainerPort, CopyToContainer, WaitFor};
use testcontainers::Image;

const GASLIGHTMQ_PORT: ContainerPort = ContainerPort::Tcp(9092);
const SCENARIO_PATH: &str = "/etc/gaslightmq/scenario.yaml";

#[derive(Debug, Clone)]
pub struct GaslightMq {
    tag: String,
    copy_to: Vec<CopyToContainer>,
    cmd: Vec<String>,
}

impl Default for GaslightMq {
    fn default() -> Self {
        Self {
            tag: "latest".to_string(),
            copy_to: Vec::new(),
            cmd: Vec::new(),
        }
    }
}

impl GaslightMq {
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tag = tag.into();
        self
    }

    fn with_scenario_yaml(mut self, yaml: String) -> Self {
        self.copy_to = vec![CopyToContainer::new(
            yaml.into_bytes(),
            SCENARIO_PATH,
        )];
        self.cmd = vec![
            "--scenario".to_string(),
            SCENARIO_PATH.to_string(),
        ];
        self
    }
}

impl Image for GaslightMq {
    fn name(&self) -> &str {
        "ghcr.io/cruxy-eu/gaslightmq"
    }

    fn tag(&self) -> &str {
        &self.tag
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::healthcheck()]
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        &[GASLIGHTMQ_PORT]
    }

    fn copy_to_sources(&self) -> impl IntoIterator<Item = &CopyToContainer> {
        &self.copy_to
    }

    fn cmd(&self) -> impl IntoIterator<Item = impl Into<std::borrow::Cow<'_, str>>> {
        &self.cmd
    }
}

impl From<Scenario> for GaslightMq {
    fn from(scenario: Scenario) -> Self {
        let yaml = scenario
            .to_yaml()
            .expect("Scenario serialization should not fail");
        Self::default().with_scenario_yaml(yaml)
    }
}

impl From<String> for GaslightMq {
    fn from(yaml: String) -> Self {
        Self::default().with_scenario_yaml(yaml)
    }
}

impl From<&str> for GaslightMq {
    fn from(yaml: &str) -> Self {
        Self::default().with_scenario_yaml(yaml.to_string())
    }
}

impl TryFrom<&Path> for GaslightMq {
    type Error = std::io::Error;

    fn try_from(path: &Path) -> Result<Self, Self::Error> {
        let yaml = std::fs::read_to_string(path)?;
        Ok(Self::default().with_scenario_yaml(yaml))
    }
}

impl TryFrom<PathBuf> for GaslightMq {
    type Error = std::io::Error;

    fn try_from(path: PathBuf) -> Result<Self, Self::Error> {
        Self::try_from(path.as_path())
    }
}
