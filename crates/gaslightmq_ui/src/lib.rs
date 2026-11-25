//! The web UI of GaslightMQ: a Dioxus application that runs in the browser.
//!
//! The UI reaches the engine only through the control plane. It shares the definition types of the engine through
//! `gaslightmq_control_api`, sends operations and follows the live stream with `gaslightmq_control_client`,
//! and turns pad presses into intents with `gaslightmq_surface`. It runs none of the engine's behaviour,
//! and the Kafka codec never reaches the browser.
//!
//! The control server serves the UI from the address of the control plane, so the page and `/api/…` share one origin.
//!
//! # What the UI knows, and what it draws
//!
//! The crate has two halves.
//!
//! | Half | Holds |
//! |---|---|
//! | What the UI knows | Plain values that apply the messages of the live stream and decide what to ask next. They never wait, and the tests drive them with plain calls. |
//! | What the UI draws | The components, and the connection that feeds the live stream to what the UI knows. They run in the browser. |
//!
//! | Type | Is |
//! |---|---|
//! | [`Live`] | Everything the UI knows from the live stream, the environment it shows, and the subscription it wants. |
//! | [`Process`] | The process as the stream tells it: the snapshot, with every change applied to it. |
//! | [`Traffic`] | The newest calls of the environment shown, as many as the window holds. |
//! | [`TrafficFilter`] | The conditions of the calls that the traffic shows, which the stream follows as a selector. |
//! | [`Following`] | What the UI asked of the stream, and what the control plane follows and refused. |
//! | [`Link`] | Whether the live stream is open. |
//! | [`UiConfiguration`] | The window of the traffic, how often the UI draws, and when it opens a new stream. |
//!
//! The module [`rules`] holds what the Rules view decides: which rules a tab lists, what someone can do with a rule,
//! and which operation carries it out. The module [`text`] writes times and the values of a call.
//!
//! # The live stream
//!
//! The UI opens the live stream when it starts. The snapshot comes first, and every message after it applies to it,
//! so the UI holds what the process holds. The UI follows the calls of the environment it shows,
//! with its filter as the selector of the subscription, so it only receives the calls it shows.
//! A stream that ends leaves what the UI knew on the screen, and the UI opens a new one, which starts with a new
//! snapshot.

mod configuration;
mod filter;
mod following;
mod live;
mod process;
pub mod rules;
pub mod text;
mod traffic;

mod app;
mod browser;
mod components;
mod connection;
mod views;

pub use app::App;
pub use configuration::UiConfiguration;
pub use filter::TrafficFilter;
pub use following::Following;
pub use live::{Link, Live};
pub use process::{Process, rule_label, rule_status};
pub use traffic::Traffic;
