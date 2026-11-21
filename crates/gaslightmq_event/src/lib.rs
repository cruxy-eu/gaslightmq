//! The event vocabulary: what changes the truth, what follows from it, and what a backend can do.
//!
//! | Type | Is |
//! |---|---|
//! | [`EventKind`] | Which of the thirteen events, without parameters. |
//! | [`EventDefinition`] | An event as a rule's action or a preset writes it. It may leave its subject to the call. |
//! | [`Subjects`] | What a call is about: the partitions, topics, broker, groups and transactional IDs it offers. |
//! | [`Event`] | A change to the truth, with everything it needs to happen. |
//! | [`Coordinator`] | Whose coordinator: the one of a group, or the one of a transactional producer. |
//! | [`PartitionCount`], [`RecordCount`] | How many partitions or records an event adds. |
//! | [`Topology`] | What an expansion asks the cluster: who leads what, and who hosts which coordinator. |
//! | [`Consequence`] | What an event brings about besides its own effect. |
//! | [`ExpandedEvent`] | An event with its consequences. |
//! | [`Support`] | What a backend does with a kind of event: apply it, emulate it, or neither. |
//! | [`Capabilities`] | The support a backend declares for every kind. |
//! | [`Requirements`], [`Requirement`] | The kinds of events a scenario needs, and why. |
//! | [`UnsupportedEvents`] | The events a scenario needs and its backend cannot do. |
//! | [`EventError`] | Why a value cannot become an event, or a part of one. |
//!
//! The [`prelude`] holds all of them.
//!
//! # From a definition to a change
//!
//! An event passes through three types on its way from a file to the cluster.
//!
//! ```
//! use gaslightmq_event::prelude::*;
//! use gaslightmq_kernel::prelude::*;
//!
//! // A rule's action: "the broker of this call goes down, and its partitions fail over".
//! let action = EventDefinition::BrokerGoesDown { broker: None, failover: true };
//!
//! // The scenario loads only on a backend that can do the event and its consequences.
//! let backend = Capabilities::none().applies(EventKind::ALL);
//! backend.check(&Requirements::from(&action))?;
//!
//! // The rule fires on a call to broker 2.
//! let call = Subjects::new().with_broker(BrokerId::from(2));
//! let events = action.resolve(&call)?;
//! assert_eq!(events, [Event::BrokerGoesDown { broker: BrokerId::from(2), failover: true }]);
//!
//! // The cluster says what broker 2 leads and hosts, and the event gets its consequences.
//! struct Cluster;
//!
//! impl Topology for Cluster {
//!     fn partitions_led_by(&self, _broker: BrokerId) -> Vec<TopicPartition> {
//!         vec!["orders.eu/3".parse().unwrap()]
//!     }
//!
//!     fn coordinators_hosted_by(&self, _broker: BrokerId) -> Vec<Coordinator> {
//!         vec![]
//!     }
//! }
//!
//! for event in events {
//!     let expanded = event.expand(&Cluster);
//!     assert_eq!(
//!         expanded.consequences(),
//!         [Consequence::Event(Event::LeaderMoves { partition: "orders.eu/3".parse()?, to: None })]
//!     );
//! }
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # The `serde` feature
//!
//! With the `serde` feature, a definition reads and writes the map a rule or a preset holds.
//! The `event` key names the kind, and the other keys are the parameters:
//!
//! ```yaml
//! { event: leader-moves, partition: orders.eu/3, to: 2 }
//! { event: broker-goes-down, broker: 2, failover: true }
//! { event: coordinator-moves, group: payments }
//! { event: partition-added, topic: orders.eu, count: 2 }
//! { event: records-expired, partition: orders.eu/3, to: 5000 }
//! { event: committed-offset-reset, group: payments, partition: orders.eu/3, to: 0 }
//! { event: producer-fenced, transactional.id: payments-tx-1 }
//! ```
//!
//! | Key | Holds | On |
//! |---|---|---|
//! | `partition` | The partition, such as `orders.eu/3`. | `leader-moves`, `records-appended`, `records-expired`, `log-truncated`, `committed-offset-reset` |
//! | `topic` | The topic. | `partition-added`, `topic-deleted` |
//! | `broker` | The broker ID. | `broker-goes-down`, `broker-goes-unresponsive`, `broker-comes-back` |
//! | `group` | The consumer group. | `rebalance`, `coordinator-moves`, `committed-offset-reset` |
//! | `transactional.id` | The transactional ID. | `coordinator-moves`, `producer-fenced` |
//! | `to` | The broker something moves to. Optional. | `leader-moves`, `coordinator-moves` |
//! | `to` | The offset something moves to. Required. | `records-expired`, `log-truncated`, `committed-offset-reset` |
//! | `count` | How many to add. One when left out. | `partition-added`, `records-appended` |
//! | `failover` | Whether leaders and coordinators move away. `false` when left out. | `broker-goes-down` |
//!
//! The keys `partition`, `topic`, `broker`, `group` and `transactional.id` name the subject.
//! A definition that leaves its subject out takes it from the call that fires the rule.
//! `coordinator-moves` takes `group` or `transactional.id`, and not both.
//!
//! Reading rejects a key that does not belong to the event.
//! A misspelt `partition` would otherwise silently mean "the partition of the call".
//!
//! The other types write these forms:
//!
//! | Type | Form |
//! |---|---|
//! | [`EventKind`] | Its name, such as `leader-moves`. |
//! | [`Event`] | The map of a definition that names every subject. |
//! | [`Consequence`] | `{ consequence: leaderless, partition: orders.eu/3 }`, `{ consequence: coordinator-unavailable, group: payments }`, or the map of the event that follows. |
//! | [`ExpandedEvent`] | The map of the event, with a `consequences` list when it has any. |
//! | [`Support`] | `applied`, `emulated` or `unsupported`. |
//! | [`Capabilities`] | A map of every kind to its support. Reading takes a kind that is left out as unsupported. |
//! | [`PartitionCount`], [`RecordCount`] | A number. |
//!
//! # No IO
//!
//! The crate depends on `gaslightmq_kernel` alone.
//! It has no async code, no IO, no clock and no randomness, and it compiles to WebAssembly.

mod capability;
mod consequence;
mod count;
mod definition;
mod error;
mod event;
#[cfg(feature = "serde")]
mod form;
mod kind;
mod requirement;

pub use capability::{Capabilities, Support, UnsupportedEvents};
pub use consequence::{Consequence, ExpandedEvent, Topology};
pub use count::{PartitionCount, RecordCount};
pub use definition::{EventDefinition, Subjects};
pub use error::EventError;
pub use event::{Coordinator, Event};
pub use kind::EventKind;
pub use requirement::{Requirement, Requirements};

/// Every type of this crate.
///
/// Import it whole wherever code speaks in events, next to the prelude of `gaslightmq_kernel`.
/// No name in it is a name of that prelude or of the standard one, so the import shadows nothing.
///
/// ```
/// use gaslightmq_event::prelude::*;
/// use gaslightmq_kernel::prelude::*;
///
/// let event = Event::LeaderMoves { partition: "orders.eu/3".parse()?, to: None };
/// assert_eq!(event.kind(), EventKind::LeaderMoves);
/// # Ok::<(), KernelError>(())
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{
        Capabilities, Consequence, Coordinator, Event, EventDefinition, EventError, EventKind, ExpandedEvent,
        PartitionCount, RecordCount, Requirement, Requirements, Subjects, Support, Topology, UnsupportedEvents,
    };
}
