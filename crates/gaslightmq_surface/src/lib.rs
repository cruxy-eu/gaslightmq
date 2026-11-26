//! Control surfaces: pad layouts, layers, pads as positions, the encoder mapping, and the intents a surface sends.
//!
//! A control surface gives someone a fast way to play rules live: a grid of pads and eight encoders,
//! in the web UI, and later in the CLI.
//! It holds no rule and no state of the engine.
//! It turns what someone does into an [`Intent`], and whoever holds the rules acts on it.
//!
//! | Type | Is |
//! |---|---|
//! | [`PadLayout`] | A grid that maps rows to selectors, columns to presets and layers to scopes. |
//! | [`LayoutName`], [`LayoutTitle`] | The identity of a layout, and the free text that says what it is for. |
//! | [`Layer`] | A page of the grid for one scope. It is a scope, and it has no name. |
//! | [`Row`], [`Column`] | The place of a selector and of a preset in the grid. |
//! | [`Pad`] | A position: a row and a column on a layer of a layout. It points at zero or one rule. |
//! | [`PadState`] | What a pad shows: no rule, a rule that is not armed, or an armed rule. |
//! | [`PlayMode`] | Latch or hold: what a press and a release do. |
//! | [`Encoder`] | One of the eight encoders, each for one parameter of a rule. |
//! | [`Steps`], [`EncoderTurn`] | How far someone turned an encoder, and the turn of one encoder. |
//! | [`EncoderMapping`] | What one step of each encoder is worth, and what a turn does to a rule. |
//! | [`Intent`] | What a surface sends: the state it wants for a rule, a hold of a pad and its release, or a change of a parameter by a turn. |
//! | [`Hold`] | One hold of a pad, which a press in hold adds and its release takes away. |
//! | [`Surface`] | One surface: its layout, the layer it shows, its mode and its focus. It turns presses and turns into intents. |
//! | [`LayoutStore`] | What keeps the pad layouts: the store contract, and which layouts use a selector or a preset. |
//!
//! The [`prelude`] holds all of them, and the error of each.
//!
//! # From a press to a rule
//!
//! ```text
//! press ──► Surface ── Intent ──► control plane ──► whoever holds the rules
//!                                                    ├─ PadLayout::rule       an empty pad gets its live rule
//!                                                    └─ EncoderMapping::apply a turn changes one parameter
//! ```
//!
//! ```
//! use gaslightmq_clock::TimeSpan;
//! use gaslightmq_lie::prelude::*;
//! use gaslightmq_rule::prelude::*;
//! use gaslightmq_selector::prelude::*;
//! use gaslightmq_surface::prelude::*;
//!
//! // A workspace has a layout, next to the selectors and the presets it names.
//! let order_producers = Selector::new("order-producers".parse().expect("a selector name"), Conditions::new())
//!     .with_title("Order producers".parse().expect("a selector title"));
//! let delay = Preset::new("delay-1s".parse().expect("a preset name"), TimingLie::new().with_delay(TimeSpan::from_secs(1)))
//!     .with_title("Delay 1 s".parse().expect("a preset title"));
//! let ingest: Layer = "order-ingest".parse().expect("a layer");
//! let layout = PadLayout::new(
//!     "order-desk".parse().expect("a layout name"),
//!     [order_producers.name().clone()],
//!     [delay.name().clone()],
//!     [Layer::all_clients(), ingest.clone()],
//! ).expect("a layout");
//! let selectors = SelectorSet::new([order_producers.clone()]).expect("a selector set");
//! let presets = PresetSet::new([delay.clone()]).expect("a preset set");
//!
//! // Someone switches to the layer of one client, and presses the pad of the delay on the order producers.
//! let mut surface = Surface::new(layout.clone());
//! assert!(surface.switch_to(&ingest));
//! let row = layout.row_of(order_producers.name()).expect("a row");
//! let column = layout.column_of(delay.name()).expect("a column");
//! let Some(Intent::Arm { pad }) = surface.press(row, column, PadState::Empty) else {
//!     unreachable!("a press of an empty pad arms it");
//! };
//!
//! // No rule has the name of the pad yet, so whoever holds the rules creates the live rule of its position, and arms it.
//! let rule = layout.rule(&pad, &selectors, &presets).expect("a pad of the layout");
//! assert_eq!(rule.name(), "delay-1s-on-order-producers-for-order-ingest");
//! assert_eq!(rule.title().map(RuleTitle::as_str), Some("Delay 1 s on Order producers for order-ingest"));
//! assert_eq!(rule.scope(), ingest.scope());
//!
//! // Someone turns the delay encoder two steps up, and the rule waits 100 ms longer.
//! let Some(Intent::Turn { turn, .. }) = surface.turn(EncoderTurn::new(Encoder::Delay, Steps::new(2))) else {
//!     unreachable!("a pad is focused");
//! };
//! let slower = EncoderMapping::new().apply(turn, &rule, &presets)?;
//! assert_eq!(slower.action(), &ActionRef::from(TimingLie::new().with_delay(TimeSpan::from_millis(1_100))));
//! # Ok::<(), TurnError>(())
//! ```
//!
//! # Who knows what
//!
//! | Question | Who answers |
//! |---|---|
//! | Which selector, preset and scope does a pad stand on? | [`PadLayout`]. |
//! | Which rule does a pad point at? | The rule that has [the name of the pad](PadLayout::rule_name), if there is one. A rule keeps its name, whatever its title becomes. |
//! | Is that rule there, and is it armed? | Whoever holds the rules. |
//! | What does a press want? | [`Surface`], from the mode of play and from what the pad shows. |
//! | What did a press do, and which hold did it add? | Whoever holds the rules, in its answer. |
//! | How many people hold a rule? | Whoever holds the rules. It disarms the rule when its last hold goes, and a disarm ends every hold. |
//! | What does a turn do to a rule? | [`EncoderMapping::apply`], on the side of whoever holds the rule. |
//! | Does a selector or a preset of a layout exist? | Whoever holds the whole workspace. |
//!
//! # Errors
//!
//! Every type that checks its value has an error of its own, named after it.
//!
//! | Type | Error | What is wrong |
//! |---|---|---|
//! | [`LayoutName`] | [`LayoutNameError`] | The text breaks a part of the [rule for names](LayoutName#the-rule-for-names). |
//! | [`LayoutTitle`] | [`LayoutTitleError`] | The text breaks a part of the [rule for titles](LayoutTitle#the-rule-for-titles). |
//! | [`Layer`] | [`LayerError`] | The text is no `client.id`. |
//! | [`PadLayout`] | [`PadLayoutError`] | The layout has no layer, or it holds a layer, a selector or a preset twice. |
//! | [`Encoder`] | [`EncoderError`] | No encoder has this name. |
//! | [`PlayMode`] | [`PlayModeError`] | No mode of play has this name. |
//!
//! What the crate does has one error, [`TurnError`]: a turn of an encoder does not change a rule,
//! because the rule has nothing for that encoder to change, or because it names a preset that does not exist.
//!
//! A position without a pad is no error.
//! A grid can be larger than its layout, so [`Surface::press`] and [`PadLayout::rule`] give `None` there.
//!
//! The message of an error is complete, so no error has a [`source`](std::error::Error::source).
//! An error that holds another one writes it into its own message, and keeps it in its `error` field.
//!
//! # The `serde` feature
//!
//! With the `serde` feature, the definitions and what a surface sends have a `serde` form.
//! The forms need a format that describes itself, such as YAML or JSON.
//!
//! | Type | Form |
//! |---|---|
//! | [`LayoutName`], [`LayoutTitle`] | The text. |
//! | [`PadLayout`] | A map with `name`, `rows`, `columns` and `layers`, and with `title` when it has one. Any other key, such as `uuid`, is an error. |
//! | [`Layer`] | `All clients`, the `client.id` of a client, or the map of a scope. |
//! | [`Row`], [`Column`] | The index, counted from zero. |
//! | [`Pad`] | A map with `layout`, `layer`, `row` and `column`. |
//! | [`Encoder`], [`PlayMode`] | The name. |
//! | [`Steps`], [`Hold`] | The number. |
//! | [`EncoderTurn`] | A map with `encoder` and `steps`. |
//! | [`EncoderMapping`] | A map of its properties. It leaves none out, and reads the default of one that is missing. |
//! | [`Intent`] | A map whose `intent` key is `arm`, `disarm`, `hold`, `release` or `turn`, with a `pad`, with a `hold` for a release, and with a `turn` for a turn. |
//!
//! ```yaml
//! name: order-desk
//! title: Order desk
//! rows: [order-producers, fetch, metadata, find-coordinator, join-and-sync, heartbeat, offset-commit, transactions]
//! columns: [error-retriable, error-fatal, delay-100ms, delay-1s, delay-10s, blackhole, disconnect, throttle]
//! layers: [All clients, order-ingest, payments-svc, analytics-etl, kafka-ui]
//! ```
//!
//! Reading rejects a key it does not know.
//! A [`PadState`] and a [`Surface`] have no form: they are what one surface shows, and they stay with it.
//!
//! # A store
//!
//! A [`PadLayout`] is a definition that a store holds under its name.
//! A [`LayoutStore`] is the store contract of `gaslightmq_definition_api` for pad layouts.
//! It also answers what only it can answer without reading every layout:
//! which layouts have a row for a selector, and which have a column for a preset.
//! The trait answers both for every store, over the layouts that the store lists.
//! The crate holds the trait, and no store.
//!
//! | Feature | Adds |
//! |---|---|
//! | `memory` | The store of pad layouts in memory: [`LayoutStore`] for the `MemoryStore` of `gaslightmq_definition_memory`. |
//!
//! # No IO
//!
//! The crate depends on the definitions of rules, selectors, triggers and lies,
//! and on `gaslightmq_field`, `gaslightmq_kernel` and `gaslightmq_clock` for the fields of a scope,
//! client IDs, error codes and time spans.
//! It does not depend on the Kafka codec.
//! It has no IO, no system clock and no randomness, and it compiles to WebAssembly.
//! Its only async code is in [`LayoutStore`]: the trait awaits the store it is handed, and nothing else.

mod encoder;
mod hold;
mod intent;
mod layer;
mod layout;
mod mapping;
mod name;
mod pad;
mod play;
#[cfg(feature = "serde")]
mod serde;
mod store;
mod surface;

pub use encoder::{Encoder, EncoderError, EncoderTurn, Steps};
pub use hold::Hold;
pub use intent::Intent;
pub use layer::{Layer, LayerError};
pub use layout::{PadLayout, PadLayoutError};
pub use mapping::{EncoderMapping, TurnError};
pub use name::{LayoutName, LayoutNameError, LayoutTitle, LayoutTitleError};
pub use pad::{Column, Pad, PadState, Row};
pub use play::{PlayMode, PlayModeError};
pub use store::LayoutStore;
pub use surface::Surface;

/// Every item of this crate.
///
/// Import it whole wherever code speaks in pads, next to the preludes of the crates a rule is made of.
/// No name in it is a name of the standard prelude or of those preludes, so the import shadows nothing.
///
/// ```
/// use gaslightmq_surface::prelude::*;
///
/// let layer: Result<Layer, LayerError> = "order-ingest".parse();
/// assert_eq!(layer?.to_string(), "order-ingest");
/// # Ok::<(), LayerError>(())
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{
        Column, Encoder, EncoderError, EncoderMapping, EncoderTurn, Hold, Intent, Layer, LayerError, LayoutName,
        LayoutNameError, LayoutStore, LayoutTitle, LayoutTitleError, Pad, PadLayout, PadLayoutError, PadState,
        PlayMode, PlayModeError, Row, Steps, Surface, TurnError,
    };
}
