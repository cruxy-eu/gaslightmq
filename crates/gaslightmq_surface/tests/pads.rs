//! Build step 4: someone plays the pads of a layout, and the surface says what to ask of the engine.
//!
//! The layout has the rows, the columns and the layers of the one in `docs/specs/domains/control-surfaces.md`.
//! Every test presses, releases or turns as a person does, and reads the intents that the surface returns.
//! The last tests stand where the engine will stand: they act on each intent, and show the surface what came of it.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

use std::collections::BTreeMap;

use gaslightmq_clock::TimeSpan;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_selector::prelude::*;
use gaslightmq_surface::prelude::*;
use gaslightmq_trigger::prelude::*;

/// The layout of an order desk, with the rows, columns and layers of the layout in the spec,
/// and the names of it that the tests play.
struct OrderDesk;

impl OrderDesk {
    const ROWS: [&'static str; 8] = [
        "order-producers",
        "fetch",
        "metadata",
        "find-coordinator",
        "join-and-sync",
        "heartbeat",
        "offset-commit",
        "transactions",
    ];
    const COLUMNS: [&'static str; 8] = [
        "error-retriable",
        "error-fatal",
        "delay-100ms",
        "delay-1s",
        "delay-10s",
        "blackhole",
        "disconnect",
        "throttle",
    ];
    const CLIENTS: [&'static str; 4] = ["order-ingest", "payments-svc", "analytics-etl", "kafka-ui"];

    fn layout() -> PadLayout {
        PadLayout::new(
            "order-desk".parse().unwrap(),
            Self::ROWS.map(|selector| selector.parse().unwrap()),
            Self::COLUMNS.map(|preset| preset.parse().unwrap()),
            std::iter::once(Layer::all_clients()).chain(Self::CLIENTS.map(|client| client.parse().unwrap())),
        )
        .unwrap()
        .with_title("Order desk".parse().unwrap())
    }

    fn order_producers() -> SelectorName {
        Self::ROWS[0].parse().unwrap()
    }

    fn metadata() -> SelectorName {
        Self::ROWS[2].parse().unwrap()
    }

    fn delay_1s() -> PresetName {
        Self::COLUMNS[3].parse().unwrap()
    }

    fn blackhole() -> PresetName {
        Self::COLUMNS[5].parse().unwrap()
    }

    /// The layer of the client `order-ingest`.
    fn order_ingest() -> Layer {
        Self::CLIENTS[0].parse().unwrap()
    }

    /// The layer of the client `payments-svc`.
    fn payments() -> Layer {
        Self::CLIENTS[1].parse().unwrap()
    }

    /// The selectors of the workspace that the tests play, each with its title.
    fn selectors() -> SelectorSet {
        SelectorSet::new([
            Selector::new(Self::order_producers(), Conditions::new()).with_title("Order producers".parse().unwrap()),
            Selector::new(Self::metadata(), Conditions::new()).with_title("Metadata".parse().unwrap()),
        ])
        .unwrap()
    }

    /// The presets of the workspace that the tests play, each with its title.
    fn presets() -> PresetSet {
        let second = TimingLie::new().with_delay(TimeSpan::from_secs(1));
        PresetSet::new([
            Preset::new(Self::delay_1s(), second).with_title("Delay 1 s".parse().unwrap()),
            Preset::new(Self::blackhole(), LieDefinition::Silence).with_title("Blackhole".parse().unwrap()),
        ])
        .unwrap()
    }

    /// Where the pad of `delay-1s` on `order-producers` stands: the first row, and the fourth column.
    fn slow_order_producers() -> (Row, Column) {
        let layout = Self::layout();
        (
            layout.row_of(&Self::order_producers()).unwrap(),
            layout.column_of(&Self::delay_1s()).unwrap(),
        )
    }

    /// Where the pad of `blackhole` on `metadata` stands.
    fn silent_metadata() -> (Row, Column) {
        let layout = Self::layout();
        (
            layout.row_of(&Self::metadata()).unwrap(),
            layout.column_of(&Self::blackhole()).unwrap(),
        )
    }

    /// A position past the last row, where a grid that is larger than the layout still has a pad to press.
    fn past_the_grid() -> (Row, Column) {
        (Row::from_index(Self::ROWS.len()), Column::from_index(0))
    }
}

/// What stands where the engine will stand: the live rules by name, next to the definitions of the workspace.
///
/// A pad points at the rule that has the name of the pad, as every reference to a rule goes by its name.
struct LiveRules {
    layout: PadLayout,
    selectors: SelectorSet,
    presets: PresetSet,
    encoders: EncoderMapping,
    by_name: BTreeMap<RuleName, (Rule, RuleState)>,
}

impl LiveRules {
    /// No live rule yet.
    fn of(layout: PadLayout) -> Self {
        Self {
            layout,
            selectors: OrderDesk::selectors(),
            presets: OrderDesk::presets(),
            encoders: EncoderMapping::new(),
            by_name: BTreeMap::new(),
        }
    }

    /// Does what the intent asks for, as the engine does when a surface sends it.
    fn act_on(&mut self, intent: Intent) {
        let name = self.layout.rule_name(intent.pad()).unwrap();
        match intent {
            Intent::Arm { pad } => {
                let of_the_position = self.layout.rule(&pad, &self.selectors, &self.presets).unwrap();
                let (_, state) = self.by_name.entry(name).or_insert((of_the_position, RuleState::Draft));
                *state = state.arm().unwrap();
            }
            Intent::Disarm { .. } => {
                let (_, state) = self.by_name.get_mut(&name).unwrap();
                *state = state.disarm().unwrap();
            }
            Intent::Turn { turn, .. } => {
                let (rule, _) = self.by_name.get_mut(&name).unwrap();
                *rule = self.encoders.apply(turn, rule, &self.presets).unwrap();
            }
        }
    }

    /// Someone gives the rule behind the pad another title, in the list of rules.
    fn retitle(&mut self, pad: &Pad, title: RuleTitle) {
        let (rule, _) = self.by_name.get_mut(&self.layout.rule_name(pad).unwrap()).unwrap();
        *rule = rule.clone().with_title(title);
    }

    fn rule(&self, pad: &Pad) -> Option<&Rule> {
        let name = self.layout.rule_name(pad)?;
        self.by_name.get(&name).map(|(rule, _)| rule)
    }

    /// What the engine publishes about the rule of the pad, as a surface shows it.
    fn shows(&self, pad: &Pad) -> PadState {
        let state = self
            .layout
            .rule_name(pad)
            .and_then(|name| self.by_name.get(&name))
            .map(|(_, state)| *state);
        PadState::of(state)
    }
}

#[test]
fn a_press_of_an_empty_pad_arms_the_rule_of_its_row_its_column_and_its_layer() {
    let layout = OrderDesk::layout();
    let mut surface = Surface::new(layout.clone());
    assert!(surface.switch_to(&OrderDesk::order_ingest()));
    let (row, column) = OrderDesk::slow_order_producers();

    let intent = surface.press(row, column, PadState::Empty).unwrap();

    let pad = surface.pad(row, column).unwrap();
    assert_eq!(intent, Intent::Arm { pad: pad.clone() });
    assert_eq!(
        intent.to_string(),
        "arm the rule at row 1, column 4 on layer order-ingest of layout order-desk"
    );

    // No rule has the name of the pad yet, so the engine creates the live rule of that position.
    let ingest = ClientId::new(OrderDesk::CLIENTS[0]).unwrap();
    assert_eq!(
        layout.rule(&pad, &OrderDesk::selectors(), &OrderDesk::presets()),
        Some(
            Rule::new(
                "delay-1s-on-order-producers-for-order-ingest".parse().unwrap(),
                OrderDesk::order_producers(),
                OrderDesk::delay_1s(),
            )
            .with_title("Delay 1 s on Order producers for order-ingest".parse().unwrap())
            .scoped(Scope::client(ingest))
        )
    );
}

#[test]
fn in_latch_a_press_toggles_the_pad_and_a_release_does_nothing() {
    let mut surface = Surface::new(OrderDesk::layout());
    let (row, column) = OrderDesk::slow_order_producers();
    let pad = surface.pad(row, column).unwrap();

    assert_eq!(surface.mode(), PlayMode::Latch);
    assert_eq!(
        surface.press(row, column, PadState::Empty),
        Some(Intent::Arm { pad: pad.clone() })
    );
    assert_eq!(surface.release(row, column), None);
    assert_eq!(
        surface.press(row, column, PadState::Armed),
        Some(Intent::Disarm { pad: pad.clone() })
    );
    assert_eq!(surface.release(row, column), None);
    assert_eq!(
        surface.press(row, column, PadState::Disarmed),
        Some(Intent::Arm { pad })
    );
}

#[test]
fn in_hold_a_pad_is_armed_while_pressed_and_disarms_on_release() {
    let mut surface = Surface::new(OrderDesk::layout());
    surface.set_mode(PlayMode::Hold);
    let (row, column) = OrderDesk::slow_order_producers();
    let pad = surface.pad(row, column).unwrap();

    for shows in [PadState::Empty, PadState::Disarmed] {
        assert_eq!(
            surface.press(row, column, shows),
            Some(Intent::Arm { pad: pad.clone() })
        );
        assert_eq!(surface.release(row, column), Some(Intent::Disarm { pad: pad.clone() }));
        // The pad is not held any more, so a second release has nothing to disarm.
        assert_eq!(surface.release(row, column), None);
    }
}

#[test]
fn a_pad_that_is_armed_already_stays_armed_while_held_and_disarms_on_release() {
    let mut surface = Surface::new(OrderDesk::layout());
    surface.set_mode(PlayMode::Hold);
    let (row, column) = OrderDesk::slow_order_producers();
    let pad = surface.pad(row, column).unwrap();

    assert_eq!(surface.press(row, column, PadState::Armed), None);
    assert_eq!(surface.release(row, column), Some(Intent::Disarm { pad }));
}

#[test]
fn a_release_disarms_the_pad_that_was_pressed_whatever_the_surface_shows_by_then() {
    let mut surface = Surface::new(OrderDesk::layout());
    surface.set_mode(PlayMode::Hold);
    assert!(surface.switch_to(&OrderDesk::order_ingest()));
    let (row, column) = OrderDesk::slow_order_producers();
    let pressed = surface.pad(row, column).unwrap();
    assert_eq!(
        surface.press(row, column, PadState::Empty),
        Some(Intent::Arm { pad: pressed.clone() })
    );

    // With the pad still down, someone switches the layer and the mode.
    assert!(surface.switch_to(&OrderDesk::payments()));
    surface.set_mode(PlayMode::Latch);

    assert_eq!(pressed.layer(), &OrderDesk::order_ingest());
    assert_eq!(surface.release(row, column), Some(Intent::Disarm { pad: pressed }));
}

#[test]
fn two_held_pads_disarm_one_by_one() {
    let mut surface = Surface::new(OrderDesk::layout());
    surface.set_mode(PlayMode::Hold);
    let (slow, silent) = (OrderDesk::slow_order_producers(), OrderDesk::silent_metadata());
    let slow_pad = surface.pad(slow.0, slow.1).unwrap();
    let silent_pad = surface.pad(silent.0, silent.1).unwrap();

    assert!(surface.press(slow.0, slow.1, PadState::Empty).is_some());
    assert!(surface.press(silent.0, silent.1, PadState::Empty).is_some());

    assert_eq!(surface.release(slow.0, slow.1), Some(Intent::Disarm { pad: slow_pad }));
    assert_eq!(
        surface.release(silent.0, silent.1),
        Some(Intent::Disarm { pad: silent_pad })
    );
    assert_eq!(surface.release(silent.0, silent.1), None);
}

#[test]
fn a_position_without_a_pad_is_dead() {
    let mut surface = Surface::new(OrderDesk::layout());
    let (row, column) = OrderDesk::past_the_grid();

    for mode in PlayMode::ALL {
        surface.set_mode(mode);
        assert_eq!(surface.pad(row, column), None);
        assert_eq!(surface.press(row, column, PadState::Empty), None);
        assert_eq!(surface.release(row, column), None);
        assert_eq!(surface.focus(row, column), None);
        assert_eq!(surface.focused(), None);
    }
}

#[test]
fn a_new_surface_shows_the_first_layer_and_switches_to_another_one_of_its_layout() {
    let mut surface = Surface::new(OrderDesk::layout());
    let (row, column) = OrderDesk::slow_order_producers();
    assert_eq!(surface.layer(), &Layer::all_clients());
    assert_eq!(surface.pad(row, column).unwrap().layer(), &Layer::all_clients());

    assert!(surface.switch_to(&OrderDesk::payments()));
    assert_eq!(surface.layer(), &OrderDesk::payments());
    assert_eq!(surface.pad(row, column).unwrap().layer(), &OrderDesk::payments());

    // The layout has no layer for this client, so the surface stays where it is.
    let unknown: Layer = "inventory-sync".parse().unwrap();
    assert!(!surface.switch_to(&unknown));
    assert_eq!(surface.layer(), &OrderDesk::payments());
}

#[test]
fn the_encoders_tune_the_pad_that_was_pressed_last() {
    let mut surface = Surface::new(OrderDesk::layout());
    let turn = EncoderTurn::new(Encoder::Probability, Steps::new(3));
    let (slow, silent) = (OrderDesk::slow_order_producers(), OrderDesk::silent_metadata());

    // Nothing is focused before the first press.
    assert_eq!(surface.focused(), None);
    assert_eq!(surface.turn(turn), None);

    surface.press(slow.0, slow.1, PadState::Empty);
    let slow_pad = surface.pad(slow.0, slow.1).unwrap();
    assert_eq!(surface.focused(), Some(slow_pad.clone()));
    let intent = surface.turn(turn).unwrap();
    assert_eq!(intent, Intent::Turn { pad: slow_pad, turn });
    assert_eq!(
        intent.to_string(),
        "change probability by +3 for the rule at row 1, column 4 on layer All clients of layout order-desk"
    );

    surface.press(silent.0, silent.1, PadState::Empty);
    let silent_pad = surface.pad(silent.0, silent.1).unwrap();
    assert_eq!(surface.turn(turn), Some(Intent::Turn { pad: silent_pad, turn }));
}

#[test]
fn a_pad_takes_the_focus_without_a_press_and_keeps_it_across_layers() {
    let mut surface = Surface::new(OrderDesk::layout());
    let turn = EncoderTurn::new(Encoder::Delay, Steps::new(-1));
    let (row, column) = OrderDesk::slow_order_producers();

    let focused = surface.focus(row, column).unwrap();
    assert_eq!(surface.focused(), Some(focused.clone()));
    assert_eq!(surface.turn(turn), Some(Intent::Turn { pad: focused, turn }));

    // The focus is a position, so on another layer it is the pad at the same row and column.
    assert!(surface.switch_to(&OrderDesk::order_ingest()));
    let on_ingest = surface.pad(row, column).unwrap();
    assert_eq!(on_ingest.layer(), &OrderDesk::order_ingest());
    assert_eq!(surface.focused(), Some(on_ingest.clone()));
    assert_eq!(surface.turn(turn), Some(Intent::Turn { pad: on_ingest, turn }));
}

#[test]
fn a_surface_follows_a_layout_that_someone_changed() {
    let mut surface = Surface::new(OrderDesk::layout());
    assert!(surface.switch_to(&OrderDesk::payments()));
    let (row, column) = OrderDesk::silent_metadata();
    surface.focus(row, column).unwrap();

    // Someone adds a row at the end. The surface stays on its layer, and keeps its focus.
    let longer = PadLayout::new(
        OrderDesk::layout().name().clone(),
        OrderDesk::layout()
            .rows()
            .iter()
            .cloned()
            .chain(["admin-calls".parse().unwrap()]),
        OrderDesk::layout().columns().to_vec(),
        OrderDesk::layout().layers().to_vec(),
    )
    .unwrap();
    surface.show(longer.clone());
    assert_eq!(surface.layout(), &longer);
    assert_eq!(surface.layer(), &OrderDesk::payments());
    assert_eq!(surface.focused(), surface.pad(row, column));

    // Someone takes the layers of the clients away, and all rows but the first.
    let smaller = PadLayout::new(
        OrderDesk::layout().name().clone(),
        [OrderDesk::order_producers()],
        OrderDesk::layout().columns().to_vec(),
        [Layer::all_clients()],
    )
    .unwrap();
    surface.show(smaller);
    assert_eq!(surface.layer(), &Layer::all_clients());
    assert_eq!(surface.focused(), None);
    assert_eq!(
        surface.turn(EncoderTurn::new(Encoder::Probability, Steps::new(1))),
        None
    );
}

// The tests from here on act on the intents, where the engine will stand.

#[test]
fn someone_arms_a_pad_tunes_its_rule_and_takes_it_back() {
    let mut surface = Surface::new(OrderDesk::layout());
    let mut live = LiveRules::of(OrderDesk::layout());
    let (row, column) = OrderDesk::slow_order_producers();
    let pad = surface.pad(row, column).unwrap();
    assert_eq!(live.shows(&pad), PadState::Empty);

    // The first press creates the rule of the position, and arms it.
    live.act_on(surface.press(row, column, live.shows(&pad)).unwrap());
    assert_eq!(live.shows(&pad), PadState::Armed);
    assert_eq!(
        live.rule(&pad),
        OrderDesk::layout()
            .rule(&pad, &OrderDesk::selectors(), &OrderDesk::presets())
            .as_ref()
    );

    // Three steps down on the first encoder: the rule fires on 97% of its matching calls, and stays armed.
    let rarer = EncoderTurn::new(Encoder::Probability, Steps::new(-3));
    live.act_on(surface.turn(rarer).unwrap());
    let chance = live
        .rule(&pad)
        .unwrap()
        .trigger()
        .parts()
        .and_then(TriggerParts::chance);
    assert_eq!(chance, Some(Chance::from_percent(97).unwrap()));
    assert_eq!(live.shows(&pad), PadState::Armed);

    // Two steps up on the delay: the rule has a lie of its own now, and the preset is what it was.
    let slower = EncoderTurn::new(Encoder::Delay, Steps::new(2));
    live.act_on(surface.turn(slower).unwrap());
    let delay = TimeSpan::from_secs(1).as_millis() + 2 * EncoderMapping::new().delay_step().as_millis();
    assert_eq!(
        live.rule(&pad).unwrap().action(),
        &ActionRef::from(TimingLie::new().with_delay(TimeSpan::from_millis(delay)))
    );

    // The second press disarms the rule, and the third arms the same rule again, with what the encoders did to it.
    let tuned = live.rule(&pad).cloned();
    live.act_on(surface.press(row, column, live.shows(&pad)).unwrap());
    assert_eq!(live.shows(&pad), PadState::Disarmed);
    live.act_on(surface.press(row, column, live.shows(&pad)).unwrap());
    assert_eq!(live.shows(&pad), PadState::Armed);
    assert_eq!(live.rule(&pad).cloned(), tuned);
}

#[test]
fn a_pad_points_at_its_rule_whatever_title_someone_gives_the_rule() {
    let mut surface = Surface::new(OrderDesk::layout());
    let mut live = LiveRules::of(OrderDesk::layout());
    let (row, column) = OrderDesk::silent_metadata();
    let pad = surface.pad(row, column).unwrap();
    live.act_on(surface.press(row, column, live.shows(&pad)).unwrap());
    let generated = live.rule(&pad).unwrap().clone();
    assert_eq!(generated.name(), "blackhole-on-metadata-for-all-clients");
    assert_eq!(
        generated.title().map(RuleTitle::as_str),
        Some("Blackhole on Metadata for All clients")
    );

    let stale: RuleTitle = "Stale metadata forever".parse().unwrap();
    live.retitle(&pad, stale.clone());

    // The rule keeps its name, so the next press of the pad still reaches it, and disarms it.
    live.act_on(surface.press(row, column, live.shows(&pad)).unwrap());
    assert_eq!(live.shows(&pad), PadState::Disarmed);
    assert_eq!(live.rule(&pad), Some(&generated.with_title(stale)));
}

#[test]
fn the_same_position_holds_another_rule_on_another_layer() {
    let mut surface = Surface::new(OrderDesk::layout());
    let mut live = LiveRules::of(OrderDesk::layout());
    let (row, column) = OrderDesk::slow_order_producers();

    let for_everyone = surface.pad(row, column).unwrap();
    live.act_on(surface.press(row, column, live.shows(&for_everyone)).unwrap());

    assert!(surface.switch_to(&OrderDesk::order_ingest()));
    let for_ingest = surface.pad(row, column).unwrap();
    assert_eq!(live.shows(&for_ingest), PadState::Empty);
    live.act_on(surface.press(row, column, live.shows(&for_ingest)).unwrap());

    assert!(live.rule(&for_everyone).unwrap().scope().is_all());
    assert_eq!(
        live.rule(&for_ingest).unwrap().scope(),
        OrderDesk::order_ingest().scope()
    );
    assert_eq!(live.shows(&for_everyone), PadState::Armed);
    assert_eq!(live.shows(&for_ingest), PadState::Armed);
}
