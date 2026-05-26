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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_has_no_scenario() {
        let gmq = GaslightMq::default();
        assert_eq!(gmq.tag, "latest");
        assert!(gmq.copy_to.is_empty());
        assert!(gmq.cmd.is_empty());
    }

    #[test]
    fn from_string_sets_scenario() {
        let yaml = "topics:\n  - name: t\n    mode: buffer".to_string();
        let gmq = GaslightMq::from(yaml);
        assert_eq!(gmq.copy_to.len(), 1);
        assert_eq!(gmq.cmd, vec!["--scenario", SCENARIO_PATH]);
    }

    #[test]
    fn from_str_sets_scenario() {
        let gmq = GaslightMq::from("topics:\n  - name: t\n    mode: buffer");
        assert_eq!(gmq.copy_to.len(), 1);
        assert_eq!(gmq.cmd, vec!["--scenario", SCENARIO_PATH]);
    }

    #[test]
    fn from_scenario_struct() {
        let scenario = Scenario::from_yaml("topics:\n  - name: t\n    mode: buffer").unwrap();
        let gmq = GaslightMq::from(scenario);
        assert_eq!(gmq.copy_to.len(), 1);
        assert_eq!(gmq.cmd, vec!["--scenario", SCENARIO_PATH]);
    }

    #[test]
    fn with_tag_overrides_default() {
        let gmq = GaslightMq::default().with_tag("0.2.0");
        assert_eq!(gmq.tag, "0.2.0");
    }

    #[test]
    fn try_from_path_reads_file() {
        let dir = std::env::temp_dir().join("gaslightmq-test-scenario.yaml");
        std::fs::write(&dir, "topics:\n  - name: t\n    mode: buffer").unwrap();

        let gmq = GaslightMq::try_from(dir.as_path()).unwrap();
        assert_eq!(gmq.copy_to.len(), 1);

        std::fs::remove_file(&dir).ok();
    }

    #[test]
    fn try_from_path_returns_error_for_missing_file() {
        let result = GaslightMq::try_from(Path::new("/nonexistent/scenario.yaml"));
        assert!(result.is_err());
    }

    #[test]
    fn image_name_and_tag() {
        let gmq = GaslightMq::default();
        assert_eq!(gmq.name(), "ghcr.io/cruxy-eu/gaslightmq");
        assert_eq!(gmq.tag(), "latest");
    }

    #[test]
    fn image_exposes_9092() {
        let gmq = GaslightMq::default();
        assert_eq!(gmq.expose_ports(), &[GASLIGHTMQ_PORT]);
    }
}
