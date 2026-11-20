//! Control-plane types shared by `gaslightmq-proxy` and its web UI.
//!
//! Serde only, so it compiles to wasm.

pub mod catalog;
pub mod model;
pub mod msg;

pub use catalog::{ApiCatalogEntry, ErrorCodeInfo, api_key_by_name, api_name, catalog, error_name};
pub use model::{
    ApiRow, ApiTick, BootstrapInfo, DelayPad, ErrorPad, NodeInfo, NodeState, NodeTick, Pad,
    Percent, RuleSet, SetNodeState, Snapshot, StatsTick,
};
pub use msg::{ClientMsg, ServerMsg};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rules_yaml_accepts_api_names() {
        let yaml = r#"
rows:
  - api: Produce
    error: { enabled: true, code: 6 }
  - api_key: 32
    topic: orders
    delay: { enabled: true, ms: 2000, probability: 25 }
"#;
        let rules: RuleSet = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(rules.rows[0].api_key, catalog::api::PRODUCE);
        assert_eq!(
            rules.rows[0].error,
            ErrorPad { enabled: true, probability: 100, code: 6 }
        );
        assert_eq!(rules.rows[1].api_key, catalog::api::DESCRIBE_CONFIGS);
        assert_eq!(rules.rows[1].delay.probability, 25);
        assert_eq!(rules.rows[1].topic.as_deref(), Some("orders"));
    }

    #[test]
    fn rules_yaml_rejects_unknown_api() {
        let yaml = "rows:\n  - api: Bogus\n";
        assert!(serde_yaml::from_str::<RuleSet>(yaml).is_err());
    }

    #[test]
    fn upsert_keeps_rows_sorted_and_drops_defaults() {
        let mut rules = RuleSet::default();
        let mut produce = ApiRow::new(0);
        produce.blackhole.enabled = true;
        let mut metadata = ApiRow::new(3);
        metadata.delay.enabled = true;
        rules.upsert(metadata.clone());
        rules.upsert(produce.clone());
        assert_eq!(rules.rows.iter().map(|r| r.api_key).collect::<Vec<_>>(), [0, 3]);

        // A disabled pad with a changed knob is kept.
        produce.blackhole.enabled = false;
        produce.delay.ms = 1234;
        rules.upsert(produce.clone());
        assert_eq!(rules.rows, vec![produce, metadata.clone()]);

        // Back to defaults: gone.
        rules.upsert(ApiRow::new(0));
        assert_eq!(rules.rows, vec![metadata]);
    }

    #[test]
    fn node_state_cycles() {
        assert_eq!(NodeState::Live.next(), NodeState::Unresponsive);
        assert_eq!(NodeState::Unresponsive.next(), NodeState::Down);
        assert_eq!(NodeState::Down.next(), NodeState::Live);
    }
}
