//! The pads someone plays in the UI: a press sends the intent that the pad's rule needs, an encoder turns only a live
//! rule whose action has its parameter, and the desk follows the layouts of the workspace.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

use std::collections::BTreeMap;

use gaslightmq_control_api::clock::TimeSpan;
use gaslightmq_control_api::incident::IncidentSupport;
use gaslightmq_control_api::kernel::{ApiKey, ErrorCode};
use gaslightmq_control_api::lie::{RefusalLie, TimingLie};
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::{Preset, PresetSet, Rule, RuleState};
use gaslightmq_control_api::selector::{Condition, Conditions, Selector, SelectorSet};
use gaslightmq_control_api::surface::{Column, Encoder, Intent, Layer, PadLayout, PlayMode, Row, Steps};
use gaslightmq_ui::pads::{Desk, Move};

/// The order desk of the specs, cut down: two rows, a delay and a refusal, and two layers.
struct OrderDesk;

impl OrderDesk {
    fn selectors() -> [Selector; 2] {
        [
            Selector::new(
                "order-producers".parse().unwrap(),
                Conditions::from(Condition::is(ApiKey::PRODUCE)),
            ),
            Selector::new(
                "fetches".parse().unwrap(),
                Conditions::from(Condition::is(ApiKey::FETCH)),
            ),
        ]
    }

    fn presets() -> [Preset; 2] {
        [
            Preset::new(
                "delay-1s".parse().unwrap(),
                TimingLie::new().with_delay(TimeSpan::from_secs(1)),
            ),
            Preset::new(
                "error-retriable".parse().unwrap(),
                RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER),
            ),
        ]
    }

    fn layout() -> PadLayout {
        PadLayout::new(
            "order-desk".parse().unwrap(),
            Self::selectors().map(|selector| selector.name().clone()),
            Self::presets().map(|preset| preset.name().clone()),
            [Layer::all_clients(), "order-ingest".parse().unwrap()],
        )
        .unwrap()
    }

    /// The live rule that a press of the pad at that row and column makes, on the first layer.
    fn rule(row: usize, column: usize) -> Rule {
        let layout = Self::layout();
        let pad = layout
            .pad(&Layer::all_clients(), Row::from_index(row), Column::from_index(column))
            .unwrap();
        let selectors = SelectorSet::new(Self::selectors()).unwrap();
        let presets = PresetSet::new(Self::presets()).unwrap();
        layout.rule(&pad, &selectors, &presets).unwrap()
    }

    /// The environment `orders-sim`, with these rules.
    fn running(rules: Vec<RuleStatus>) -> RunningEnvironment {
        RunningEnvironment {
            environment: "orders-sim".parse().unwrap(),
            backend: BackendStatus {
                kind: BackendKind::Simulated,
                endpoints: BTreeMap::new(),
                incidents: IncidentSupport::none(),
            },
            rules,
            sessions: Vec::new(),
        }
    }
}

#[test]
fn a_press_of_an_empty_pad_arms_it_and_a_press_of_an_armed_pad_disarms_it() {
    let mut desk = Desk::new(OrderDesk::layout());
    let empty = OrderDesk::running(Vec::new());
    let armed = OrderDesk::running(vec![RuleStatus {
        rule: OrderDesk::rule(0, 0),
        live: true,
        state: RuleState::Armed,
    }]);

    let first = desk.press(Row::from_index(0), Column::from_index(0), &empty).unwrap();
    let second = desk.press(Row::from_index(0), Column::from_index(0), &armed).unwrap();

    assert!(matches!(first.intent, Intent::Arm { .. }), "{first:?}");
    assert!(matches!(second.intent, Intent::Disarm { .. }), "{second:?}");
    assert_eq!(first.environment, armed.environment);
    assert_eq!(
        desk.release(Row::from_index(0), Column::from_index(0), &armed),
        None,
        "latch ignores a release"
    );
}

#[test]
fn in_hold_a_pad_is_armed_while_pressed_and_disarms_on_release() {
    let mut desk = Desk::new(OrderDesk::layout());
    desk.set_mode(PlayMode::Hold);
    let empty = OrderDesk::running(Vec::new());

    let pressed = desk.press(Row::from_index(1), Column::from_index(0), &empty).unwrap();
    let released = desk.release(Row::from_index(1), Column::from_index(0), &empty).unwrap();

    assert!(matches!(pressed.intent, Intent::Arm { .. }), "{pressed:?}");
    assert!(matches!(released.intent, Intent::Disarm { .. }), "{released:?}");
}

#[test]
fn an_encoder_turns_a_live_rule_whose_action_has_its_parameter() {
    let mut desk = Desk::new(OrderDesk::layout());
    let presets = OrderDesk::presets();
    let live = OrderDesk::running(vec![RuleStatus {
        rule: OrderDesk::rule(0, 0),
        live: true,
        state: RuleState::Armed,
    }]);
    desk.focus(Row::from_index(0), Column::from_index(0));

    let slower = desk.turn(Encoder::Delay, Steps::new(2), &live, &presets).unwrap();

    assert!(matches!(slower.intent, Intent::Turn { .. }), "{slower:?}");
    assert!(desk.turns(Encoder::Probability, &live, &presets));
    assert!(
        !desk.turns(Encoder::ErrorCode, &live, &presets),
        "a delay has no error code"
    );
}

#[test]
fn a_kept_rule_and_an_empty_pad_refuse_every_turn() {
    let mut desk = Desk::new(OrderDesk::layout());
    let presets = OrderDesk::presets();
    let kept = OrderDesk::running(vec![RuleStatus {
        rule: OrderDesk::rule(0, 0),
        live: false,
        state: RuleState::Armed,
    }]);
    desk.focus(Row::from_index(0), Column::from_index(0));

    assert_eq!(desk.turn(Encoder::Delay, Steps::new(1), &kept, &presets), None);
    desk.focus(Row::from_index(1), Column::from_index(1));
    assert_eq!(desk.turn(Encoder::ErrorCode, Steps::new(1), &kept, &presets), None);
}

#[test]
fn the_focus_moves_one_pad_and_stays_at_the_edge() {
    let mut desk = Desk::new(OrderDesk::layout());
    let focused = |desk: &Desk| {
        let pad = desk.surface().focused().unwrap();
        (pad.row().index(), pad.column().index())
    };

    desk.move_focus(Move::Down);
    assert_eq!(focused(&desk), (1, 0));
    desk.move_focus(Move::Down);
    desk.move_focus(Move::Left);
    assert_eq!(focused(&desk), (1, 0));
    desk.move_focus(Move::Right);
    desk.move_focus(Move::Up);
    assert_eq!(focused(&desk), (0, 1));
}

#[test]
fn a_pad_on_one_layer_shows_that_the_same_pad_is_armed_on_another() {
    let mut desk = Desk::new(OrderDesk::layout());
    let armed = OrderDesk::running(vec![RuleStatus {
        rule: OrderDesk::rule(0, 0),
        live: true,
        state: RuleState::Armed,
    }]);

    assert!(desk.switch_to(&"order-ingest".parse().unwrap()));

    assert!(desk.armed_elsewhere(Row::from_index(0), Column::from_index(0), &armed));
    assert!(!desk.armed_elsewhere(Row::from_index(0), Column::from_index(1), &armed));
}

#[test]
fn the_desk_follows_its_layout_and_takes_the_first_one_when_its_own_is_gone() {
    let desk = Desk::new(OrderDesk::layout());
    let other = PadLayout::new(
        "fetch-desk".parse().unwrap(),
        [OrderDesk::selectors()[1].name().clone()],
        [OrderDesk::presets()[0].name().clone()],
        [Layer::all_clients()],
    )
    .unwrap();
    let retitled = OrderDesk::layout().with_title("Order desk".parse().unwrap());

    let followed = Desk::following(Some(desk.clone()), &[other.clone(), retitled.clone()]).unwrap();
    assert_eq!(followed.layout(), &retitled);
    let replaced = Desk::following(Some(desk), std::slice::from_ref(&other)).unwrap();
    assert_eq!(replaced.layout(), &other);
    assert_eq!(Desk::following(None, &[]), None);
}
