//! The definitions that the check stores.

#![expect(
    clippy::missing_panics_doc,
    reason = "a sample is made of fixed texts that are valid, so making one never panics"
)]

use gaslightmq_definition_api::Definition;
use gaslightmq_environment::{
    Environment, EnvironmentName, EnvironmentUuid, Instances, Listeners, ProxyDefinition, Upstream,
};
use gaslightmq_kernel::ApiKey;
use gaslightmq_lie::{HangUpLie, LieDefinition};
use gaslightmq_rule::{Preset, PresetName, PresetUuid, Rule, RuleName, RuleUuid};
use gaslightmq_selector::{Condition, Conditions, Selector, SelectorName, SelectorUuid};
use gaslightmq_surface::{Layer, LayoutName, LayoutUuid, PadLayout};

/// The time someone created the definition that has a UUID, in milliseconds since the Unix epoch: 11 December 2024.
const CREATED: u64 = 0x0193_b6c2_5f3e;

/// The random bits of that UUID.
const RANDOM: [u8; 10] = [0x0a, 0x41, 0x1c, 0x2d, 0x8e, 0x4b, 0x1f, 0x6a, 0x7d, 0x10];

/// The definitions of one kind that [`assert_store_contract`](crate::assert_store_contract) stores.
///
/// | Sample | Is |
/// |---|---|
/// | [`definition`](Self::definition) | A definition as someone wrote it: without a UUID and without a title. |
/// | [`changed`](Self::changed) | The same definition after a change: it has the same name, and a UUID and a title. |
/// | [`other`](Self::other) | A definition of another name, which sorts before the first. |
///
/// Every kind has samples of its own, and [`new`](Self::new) takes the samples of a test.
///
/// ```
/// use gaslightmq_definition_api::prelude::*;
/// use gaslightmq_definition_conformance::Samples;
///
/// let rules = Samples::rules();
/// assert_eq!(rules.definition().name(), rules.changed().name());
/// assert!(rules.other().name() < rules.definition().name());
/// assert!(rules.definition().uuid().is_none());
/// assert!(rules.changed().uuid().is_some());
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Samples<D> {
    definition: D,
    changed: D,
    other: D,
}

impl<D: Definition + PartialEq> Samples<D> {
    /// The samples of a test.
    ///
    /// # Panics
    ///
    /// When the samples are not what the check needs:
    /// `changed` has another name than `definition` or is equal to it,
    /// or the name of `other` does not sort before the name of `definition`.
    pub fn new(definition: D, changed: D, other: D) -> Self {
        assert!(
            definition.name() == changed.name(),
            "the changed definition has the name of the first"
        );
        assert!(definition != changed, "the changed definition differs from the first");
        assert!(
            other.name() < definition.name(),
            "the name of the other definition sorts before the name of the first"
        );
        Self {
            definition,
            changed,
            other,
        }
    }

    /// A definition as someone wrote it.
    pub fn definition(&self) -> &D {
        &self.definition
    }

    /// The same definition after a change: it has the same name, and everything a store has to keep.
    pub fn changed(&self) -> &D {
        &self.changed
    }

    /// A definition of another name, which sorts before the first.
    pub fn other(&self) -> &D {
        &self.other
    }
}

impl Samples<Selector> {
    /// Selectors: the producers of orders, and every fetch.
    pub fn selectors() -> Self {
        let name = |name: &str| name.parse::<SelectorName>().expect("a selector name");
        let order_producers = Selector::new(
            name("order-producers"),
            Conditions::new().with(Condition::is(ApiKey::PRODUCE)),
        );
        let titled = order_producers
            .clone()
            .with_uuid(SelectorUuid::new(CREATED, RANDOM))
            .with_title("Order producers".parse().expect("a selector title"));
        let fetches = Selector::new(name("fetches"), Conditions::new().with(Condition::is(ApiKey::FETCH)));
        Self::new(order_producers, titled, fetches)
    }
}

impl Samples<Preset> {
    /// Presets: a hang-up, and a silence.
    pub fn presets() -> Self {
        let name = |name: &str| name.parse::<PresetName>().expect("a preset name");
        let hang_up = Preset::new(name("hang-up"), HangUpLie::new());
        let titled = hang_up
            .clone()
            .with_uuid(PresetUuid::new(CREATED, RANDOM))
            .with_title("Hang up".parse().expect("a preset title"));
        let blackhole = Preset::new(name("blackhole"), LieDefinition::Silence);
        Self::new(hang_up, titled, blackhole)
    }
}

impl Samples<Rule> {
    /// Rules: a silence on every Metadata call, and one on every Fetch.
    pub fn rules() -> Self {
        let name = |name: &str| name.parse::<RuleName>().expect("a rule name");
        let on = |api: ApiKey| Conditions::new().with(Condition::is(api));
        let metadata = Rule::new(name("blackhole-metadata"), on(ApiKey::METADATA), LieDefinition::Silence);
        let titled = metadata
            .clone()
            .with_uuid(RuleUuid::new(CREATED, RANDOM))
            .with_title("Blackhole Metadata".parse().expect("a rule title"));
        let fetches = Rule::new(name("blackhole-fetches"), on(ApiKey::FETCH), LieDefinition::Silence);
        Self::new(metadata, titled, fetches)
    }
}

impl Samples<PadLayout> {
    /// Pad layouts: a desk for orders, and a desk for fetches. Each has one row, one column and one layer.
    pub fn layouts() -> Self {
        let desk = |name: &str, row: &str| {
            PadLayout::new(
                name.parse::<LayoutName>().expect("a layout name"),
                [row.parse().expect("a selector name")],
                ["blackhole".parse().expect("a preset name")],
                [Layer::all_clients()],
            )
            .expect("a layout with one row, one column and one layer")
        };
        let order_desk = desk("order-desk", "order-producers");
        let titled = order_desk
            .clone()
            .with_uuid(LayoutUuid::new(CREATED, RANDOM))
            .with_title("Order desk".parse().expect("a layout title"));
        let fetch_desk = desk("fetch-desk", "fetches");
        Self::new(order_desk, titled, fetch_desk)
    }
}

impl Samples<Environment> {
    /// Environments: a proxy in front of the cluster of staging, and one in front of a cluster on this machine.
    pub fn environments() -> Self {
        let proxy = |name: &str, advertise: &str, bootstrap: &str| {
            let listen = Listeners::new(
                "0.0.0.0:9092".parse().expect("a listen address"),
                "0.0.0.0:19092-19191".parse().expect("broker listeners"),
                advertise.parse().expect("a host name"),
            )
            .expect("a bootstrap port outside the ports of the brokers");
            Environment::new(
                name.parse::<EnvironmentName>().expect("an environment name"),
                Instances::ProxyInFrontOfCluster {
                    proxy: ProxyDefinition::new(listen),
                    upstream: Upstream::new(bootstrap.parse().expect("a server address")),
                },
            )
        };
        let staging = proxy("staging-proxy", "gaslight.staging", "kafka-1.staging:9092");
        let titled = staging
            .clone()
            .with_uuid(EnvironmentUuid::new(CREATED, RANDOM))
            .with_title("Staging, behind the proxy".parse().expect("an environment title"));
        let local = proxy("local-proxy", "localhost", "localhost:9094");
        Self::new(staging, titled, local)
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_rule::RuleTitle;

    use super::*;

    /// What the check relies on, for the samples of one kind.
    fn assert_fit<D: Definition + PartialEq>(samples: &Samples<D>) {
        assert!(samples.definition().name() == samples.changed().name());
        assert!(samples.definition() != samples.changed());
        assert!(samples.other().name() < samples.definition().name());
    }

    #[test]
    fn the_samples_of_every_kind_are_what_the_check_needs() {
        assert_fit(&Samples::selectors());
        assert_fit(&Samples::presets());
        assert_fit(&Samples::rules());
        assert_fit(&Samples::layouts());
        assert_fit(&Samples::environments());
    }

    #[test]
    fn a_changed_definition_has_everything_that_a_store_has_to_keep() {
        let rules = Samples::rules();

        assert_eq!(rules.definition().uuid(), None);
        assert_eq!(rules.changed().uuid(), Some(RuleUuid::new(CREATED, RANDOM)));
        assert_eq!(
            rules.changed().title().map(RuleTitle::as_str),
            Some("Blackhole Metadata")
        );
    }

    #[test]
    #[should_panic(expected = "the name of the other definition sorts before the name of the first")]
    fn samples_in_the_wrong_order_are_no_samples() {
        let rules = Samples::rules();

        // The two rules stand where the other one belongs.
        let _ = Samples::new(
            rules.other().clone(),
            rules
                .other()
                .clone()
                .without_title()
                .with_title("Blackhole fetches".parse().unwrap()),
            rules.definition().clone(),
        );
    }
}
