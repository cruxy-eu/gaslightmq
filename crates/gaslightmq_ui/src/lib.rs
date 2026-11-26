//! The web UI of GaslightMQ: a Dioxus application that runs in the browser.
//!
//! The UI reaches the engine only through the control plane. It shares the definition types of the engine through
//! `gaslightmq_control_api`, sends operations and follows the live stream with `gaslightmq_control_client`,
//! and turns pad presses into intents with `gaslightmq_surface`. It runs none of the engine's behaviour,
//! and the Kafka codec never reaches the browser.
//! Every request says which surface of the UI sends it: the pads, ⌘K, or any other view.
//! It also says which presence the tab is, so the pads that someone holds end with the tab.
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
//! | [`ThemeChoice`] | The theme of the system, light or dark, as someone picked it in the shell. |
//! | [`UiConfiguration`] | The window of the traffic, how often the UI draws, and when it opens a new stream. |
//!
//! The module [`pads`] holds the [`Desk`](pads::Desk): the surface someone plays, which turns presses and turns into
//! intents against the rules of the environment shown.
//! The module [`palette`] holds ⌘K: the sentence, the answer of the control plane to it, and the live rule it gives.
//! The module [`history`] holds the undo history of the tab: what takes back each change that the tab made, and what
//! makes it again, read from what the control plane answered.
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
//!
//! # The presence of a tab
//!
//! The live stream names the presence of the tab, and the tab keeps it in memory.
//! A stream that opens again comes back as that presence, so the pads that someone holds stay held across a
//! connection that dropped. When the control plane no longer held the presence, it ended its holds,
//! and the [`Desk`](pads::Desk) holds no pad any more.
//! A release that does not arrive goes out again until the control plane answers it.
//! A press in hold that gets no answer may have added a hold that the tab cannot name,
//! so the tab lets go of its presence, and the control plane ends whatever it held.
//! A page that loads again is a new presence: it knows nothing of the pads that the page before it held.
//!
//! # Undo and redo
//!
//! ⌘Z takes back the last change that someone made in this tab, and ⇧⌘Z makes it again. The tab keeps the history in
//! memory, and the engine keeps none: an undo is an ordinary operation, the inverse of the change, with the cause
//! `undo`. Every change of the UI expects the run and the revision that the tab shows, and an undo expects what the
//! tab left. When someone else changed the same thing since, the control plane refuses the undo, the tab drops the
//! step, and says so.

mod configuration;
mod filter;
mod following;
pub mod history;
mod live;
pub mod pads;
pub mod palette;
mod process;
pub mod rules;
pub mod text;
mod theme;
mod traffic;

mod app;
mod browser;
mod components;
mod connection;
mod style;
mod views;

pub use app::App;
pub use configuration::UiConfiguration;
pub use filter::TrafficFilter;
pub use following::Following;
pub use live::{Link, Live};
pub use process::{Process, WorkspaceDefinitions, rule_label, rule_status};
pub use theme::ThemeChoice;
pub use traffic::Traffic;
