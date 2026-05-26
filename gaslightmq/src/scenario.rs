use std::collections::HashMap;
use std::time::Duration;

use gaslightmq_scenario::{
    AfterSequenceRef, ApiRef, BehaviorRef, ChaosConfig, ErrorCode, GeneratorConfig, Scenario,
    TopicMode,
};
use kafka_protocol::error::ResponseError;

use crate::builder::BrokerBuilder;
use crate::chaos::{AfterSequence, ChaosAction, Sequence, Step};
use crate::generator::{JsonTemplate, RandomBytes};
use crate::layer::{ChaosLayer, GeneratorLayer};
use crate::record::Api;
use crate::topic::{BlackHole, Buffer, Topic};

impl From<Scenario> for BrokerBuilder {
    fn from(scenario: Scenario) -> Self {
        let mut topics: HashMap<String, Box<dyn Topic>> = HashMap::new();

        for topic_cfg in scenario.topics {
            let base: Box<dyn Topic> = match topic_cfg.mode {
                TopicMode::Buffer => Box::new(Buffer::new(topic_cfg.partitions)),
                TopicMode::BlackHole => Box::new(BlackHole::new(topic_cfg.partitions)),
            };

            let mut topic: Box<dyn Topic> = base;
            for chaos in &topic_cfg.chaos {
                let api: Api = chaos.api.into();
                let sequence = build_sequence_from_chaos(chaos);
                topic = Box::new(ChaosLayer::new(topic, api, sequence));
            }

            for gen_cfg in &topic_cfg.generators {
                topic = match gen_cfg {
                    GeneratorConfig::Json { template } => {
                        Box::new(GeneratorLayer::new(topic, JsonTemplate::new(template)))
                    }
                    GeneratorConfig::RandomBytes { size } => {
                        Box::new(GeneratorLayer::new(topic, RandomBytes::new(*size)))
                    }
                };
            }

            topics.insert(topic_cfg.name, topic);
        }

        BrokerBuilder::from_topics(topics)
    }
}

fn resolve_error_code(code: &ErrorCode) -> ResponseError {
    match code {
        ErrorCode::Code(n) => {
            ResponseError::try_from_code(*n).unwrap_or(ResponseError::UnknownServerError)
        }
        ErrorCode::Name(name) => {
            for code in -1..=200i16 {
                if let Some(err) = ResponseError::try_from_code(code)
                    && err.to_string() == *name
                {
                    return err;
                }
            }
            ResponseError::UnknownServerError
        }
    }
}

impl From<ApiRef> for Api {
    fn from(r: ApiRef) -> Api {
        match r {
            ApiRef::Produce => Api::Produce,
            ApiRef::Fetch => Api::Fetch,
            ApiRef::ListOffsets => Api::ListOffsets,
            ApiRef::Metadata => Api::Metadata,
        }
    }
}

impl From<AfterSequenceRef> for AfterSequence {
    fn from(r: AfterSequenceRef) -> AfterSequence {
        match r {
            AfterSequenceRef::Loop => AfterSequence::Loop,
            AfterSequenceRef::Stop => AfterSequence::Stop,
            AfterSequenceRef::RepeatLast => AfterSequence::RepeatLast,
        }
    }
}

fn chaos_action_from_behavior(
    behavior: BehaviorRef,
    delay_ms: Option<u64>,
    error_code: Option<&ErrorCode>,
) -> ChaosAction {
    match behavior {
        BehaviorRef::Success => ChaosAction::Proceed,
        BehaviorRef::Delay => {
            let ms = delay_ms.unwrap_or(0);
            ChaosAction::Delay(Duration::from_millis(ms))
        }
        BehaviorRef::Timeout => ChaosAction::Timeout,
        BehaviorRef::Error => {
            let resolved = error_code
                .map(resolve_error_code)
                .unwrap_or(ResponseError::UnknownServerError);
            ChaosAction::Error(resolved)
        }
    }
}

fn build_sequence_from_chaos(chaos: &ChaosConfig) -> Sequence {
    if let Some(steps) = &chaos.sequence {
        let after = chaos
            .after_sequence
            .map(AfterSequence::from)
            .unwrap_or(AfterSequence::Stop);
        let steps: Vec<Step> = steps
            .iter()
            .map(|s| {
                let action =
                    chaos_action_from_behavior(s.behavior, s.delay_ms, s.error_code.as_ref());
                Step::new(action, s.repeat)
            })
            .collect();
        Sequence::new(steps, after)
    } else {
        let behavior = chaos.behavior.unwrap_or(BehaviorRef::Success);
        let action =
            chaos_action_from_behavior(behavior, chaos.delay_ms, chaos.error_code.as_ref());
        let step = Step::new(action, chaos.repeat);
        let after = chaos
            .after_sequence
            .map(AfterSequence::from)
            .unwrap_or(AfterSequence::Stop);
        Sequence::new(vec![step], after)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASIC_YAML: &str = r#"
topics:
  - name: orders
    partitions: 3
    mode: buffer
    chaos:
      - api: Fetch
        behavior: delay
        delay_ms: 2000
        repeat: 5
      - api: Produce
        behavior: error
        error_code: NotLeaderOrFollower
    generators:
      - format: json
        template: '{"id": "{{uuid}}"}'
  - name: events
    partitions: 1
    mode: blackhole
"#;

    #[test]
    fn scenario_converts_to_broker_builder() {
        let scenario = Scenario::from_yaml(BASIC_YAML).unwrap();
        let _builder = BrokerBuilder::from(scenario);
    }

    #[test]
    fn resolve_error_code_by_name() {
        let code = ErrorCode::Name("NotLeaderOrFollower".to_string());
        assert_eq!(resolve_error_code(&code), ResponseError::NotLeaderOrFollower);
    }

    #[test]
    fn resolve_error_code_by_number() {
        let code = ErrorCode::Code(6);
        assert_eq!(resolve_error_code(&code), ResponseError::NotLeaderOrFollower);
    }
}
