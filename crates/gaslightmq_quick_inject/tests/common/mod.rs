//! What the tests of sentences share: the names of one system.

use gaslightmq_quick_inject::prelude::*;

/// A shop behind a proxy, as the host of the proxy knows it: the names in its traffic and in its workspace.
///
/// `payments` is a topic and a client, so a sentence has to say which one it means.
pub struct Shop;

impl Shop {
    const TOPICS: [&str; 3] = ["orders.eu", "orders.us", "payments"];
    const GROUPS: [&str; 1] = ["analytics-etl"];
    const CLIENTS: [&str; 2] = ["order-ingest", "payments"];
    /// Each preset with its title. Whoever looks for `flap` or `election` finds a preset.
    const PRESETS: [(&str, &str); 2] = [("leader-election", "Leader election"), ("leader-flap", "Leader flap")];
    /// Each selector with its title. Whoever looks for `producers` finds the selector.
    const SELECTORS: [(&str, &str); 1] = [("order-producers", "Order producers")];

    /// Every name the shop knows.
    pub fn known() -> KnownNames {
        KnownNames::new()
            .with_topics(Self::TOPICS.map(|topic| topic.parse().unwrap()))
            .with_groups(Self::GROUPS.map(|group| group.parse().unwrap()))
            .with_clients(Self::CLIENTS.map(|client| client.parse().unwrap()))
            .with_presets(Self::PRESETS.map(|(name, title)| (name.parse().unwrap(), Some(title.parse().unwrap()))))
            .with_selectors(Self::SELECTORS.map(|(name, title)| (name.parse().unwrap(), Some(title.parse().unwrap()))))
    }
}
