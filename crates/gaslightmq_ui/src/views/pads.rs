//! The pads of a layout, played live: rows are selectors, columns are presets, and the bottom row picks the layer.

use dioxus::prelude::*;
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::{Action, Preset, PresetName, RuleState};
use gaslightmq_control_api::selector::SelectorName;
use gaslightmq_control_api::surface::{Column, Encoder, Layer, LayoutName, PadState, PlayMode, Row, Steps};
use gaslightmq_control_api::trigger::Chance;

use crate::app::Ui;
use crate::components::{Icon, Shape, lie_color};
use crate::pads::{Desk, Move, encoder_value};
use crate::process::Process;

/// The letters of the rows, as the keyboard and the row headers name them.
const ROW_KEYS: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

/// The pad grid of the layout someone plays, with its encoders and its layers.
#[component]
pub fn Pads() -> Element {
    let ui = use_context::<Ui>();
    let live = ui.live.read();
    let header = rsx! {
        div { class: "crumb", Icon { shape: Shape::Grid, size: 15 } "Pads" }
        span { class: "t4", "·" }
        span { class: "t3 small", "rows are selectors, columns are presets, the bottom row picks the layer" }
    };
    let Some(process) = live.process() else {
        return rsx! {
            div { class: "hdr", {header} }
            div { class: "empty", "Connecting to the control plane…" }
        };
    };
    let Some(running) = live.shown_running() else {
        return rsx! {
            div { class: "hdr", {header} }
            div { class: "empty", "No environment runs that the pads could play. Start one in the switcher at the bottom left." }
        };
    };
    let layouts = process.definitions().layouts.clone();
    let Some(desk) = Desk::following(ui.desk.read().clone(), &layouts) else {
        return rsx! {
            div { class: "hdr", {header} }
            div { class: "empty", "The workspace has no pad layout. A layout maps rows to selectors and columns to presets." }
        };
    };
    let layout = desk.layout().clone();
    let presets = process.definitions().presets.clone();
    let columns: Vec<(Column, PresetName)> = layout
        .columns()
        .iter()
        .enumerate()
        .map(|(index, preset)| (Column::from_index(index), preset.clone()))
        .collect();
    let rows: Vec<(Row, SelectorName)> = layout
        .rows()
        .iter()
        .enumerate()
        .map(|(index, selector)| (Row::from_index(index), selector.clone()))
        .collect();
    let mode = desk.surface().mode();
    let shown_layer = desk.surface().layer().clone();
    let focused = desk.surface().focused();
    let grid = format!("216px repeat({}, 68px)", columns.len());
    let span = columns.len();
    let layer_buttons: Vec<(Layer, usize)> = layout
        .layers()
        .iter()
        .map(|layer| (layer.clone(), armed_on(&desk, layer, running)))
        .collect();
    let others: Vec<(LayoutName, String)> = layouts
        .iter()
        .map(|each| {
            (
                each.name().clone(),
                each.title()
                    .map_or_else(|| each.name().to_string(), ToString::to_string),
            )
        })
        .collect();
    let keys = {
        let ui = ui.clone();
        move |event: KeyboardEvent| {
            let direction = match event.key() {
                Key::ArrowUp => Some(Move::Up),
                Key::ArrowDown => Some(Move::Down),
                Key::ArrowLeft => Some(Move::Left),
                Key::ArrowRight => Some(Move::Right),
                _ => None,
            };
            if let Some(direction) = direction {
                event.prevent_default();
                play(&ui, |desk, _| {
                    desk.move_focus(direction);
                    None
                });
                return;
            }
            match event.key() {
                Key::Character(text) if text == " " => {
                    event.prevent_default();
                    play(&ui, |desk, running| {
                        let pad = desk.surface().focused()?;
                        desk.press(pad.row(), pad.column(), running)
                    });
                    play(&ui, |desk, running| {
                        let pad = desk.surface().focused()?;
                        desk.release(pad.row(), pad.column(), running)
                    });
                }
                Key::Character(text) => {
                    if let Some(index) = text.parse::<usize>().ok().and_then(|digit| digit.checked_sub(1)) {
                        play(&ui, |desk, _| {
                            let layer = desk.layout().layers().get(index).cloned()?;
                            desk.switch_to(&layer);
                            None
                        });
                    }
                }
                _ => {}
            }
        }
    };
    rsx! {
        div { class: "hdr",
            {header}
            span { class: "sp" }
            if others.len() > 1 {
                div { class: "seg",
                    for (name, title) in others {
                        LayoutButton { key: "{name}", on: &name == layout.name(), name, title }
                    }
                }
            }
            div { class: "seg",
                for each in PlayMode::ALL {
                    ModeButton { key: "{each}", mode: each, on: each == mode }
                }
            }
        }
        div { class: "pads-wrap", tabindex: "0", onkeydown: keys,
            div { class: "pads", style: "grid-template-columns:{grid}",
                FocusCard {}
                div { class: "deck", style: "grid-column:2 / span {span}",
                    for encoder in Encoder::ALL {
                        Knob { key: "{encoder.name()}", encoder }
                    }
                }
                div {}
                for (column, preset) in columns.clone() {
                    ColumnHeader { key: "{column.index()}", preset }
                }
                for (row, selector) in rows {
                    RowHeader { key: "row-{row.index()}", row, selector, on: focused.as_ref().is_some_and(|pad| pad.row() == row) }
                    for (column, preset) in columns.clone() {
                        PadCell {
                            key: "{row.index()}-{column.index()}",
                            row,
                            column,
                            color: preset_color(&presets, &preset),
                            on: focused.as_ref().is_some_and(|pad| pad.row() == row && pad.column() == column),
                        }
                    }
                }
                div { class: "scopelbl",
                    span { class: "t2", "Layer" }
                    "who the grid applies to"
                }
                div { class: "layers", style: "grid-column:2 / span {span}",
                    for (index, (layer, armed)) in layer_buttons.into_iter().enumerate() {
                        LayerButton { key: "{layer}", on: layer == shown_layer, number: index + 1, layer, armed }
                    }
                }
                div { class: "legend", style: "grid-column:1 / span {span + 1}",
                    span { class: "sw2", span { class: "sq lit" } "lit = armed, fill = chance" }
                    span { class: "sw2", span { class: "sq dashed" } "dashed = rule exists, disarmed" }
                    span { class: "sw2", span { class: "sdot small" } "armed on another layer" }
                    span { class: "sw2", span { class: "kbd", "←↑→↓" } " move " span { class: "kbd", "space" } " press " span { class: "kbd", "1–9" } " layer" }
                    span { class: "sp" }
                    span { "A press of an empty pad makes its live rule and arms it. A layer is a page, not a priority." }
                }
            }
        }
    }
}

/// Changes the desk with `change`, against the environment shown, and sends the intent it gives.
fn play(ui: &Ui, change: impl FnOnce(&mut Desk, &RunningEnvironment) -> Option<SendIntent>) {
    let intent = {
        let live = ui.live.peek();
        let (Some(process), Some(running)) = (live.process(), live.shown_running()) else {
            return;
        };
        let mut slot = ui.desk;
        let Some(mut desk) = Desk::following(slot.peek().clone(), &process.definitions().layouts) else {
            return;
        };
        let intent = change(&mut desk, running);
        slot.set(Some(desk));
        intent
    };
    if let Some(intent) = intent {
        ui.send(intent);
    }
}

/// How many pads of a layer point at an armed rule.
fn armed_on(desk: &Desk, layer: &Layer, running: &RunningEnvironment) -> usize {
    let layout = desk.layout();
    (0..layout.rows().len())
        .flat_map(|row| (0..layout.columns().len()).map(move |column| (row, column)))
        .filter_map(|(row, column)| layout.pad(layer, Row::from_index(row), Column::from_index(column)))
        .filter_map(|pad| layout.rule_name(&pad))
        .filter(|name| {
            running
                .rules
                .iter()
                .any(|status| status.rule.name() == name && status.state.is_armed())
        })
        .count()
}

/// The color of the lie of a preset, for its column.
fn preset_color(presets: &[Preset], name: &PresetName) -> &'static str {
    match presets.iter().find(|preset| preset.name() == name).map(Preset::action) {
        Some(Action::Lie(lie)) => lie_color(lie),
        _ => "#9ca3af",
    }
}

/// The title of a definition where it has one, and its name where it has none.
fn titled(title: Option<String>, name: &impl ToString) -> String {
    title.unwrap_or_else(|| name.to_string())
}

/// One layout of the workspace, which the desk plays when someone picks it.
#[component]
fn LayoutButton(name: LayoutName, title: String, on: bool) -> Element {
    let ui = use_context::<Ui>();
    let pick = move |_| {
        let live = ui.live.peek();
        let layout = live.process().and_then(|process| {
            process
                .definitions()
                .layouts
                .iter()
                .find(|layout| layout.name() == &name)
                .cloned()
        });
        if let Some(layout) = layout {
            let mut slot = ui.desk;
            slot.set(Some(Desk::new(layout)));
        }
    };
    rsx! {
        button { class: if on { "on" } else { "" }, onclick: pick, "{title}" }
    }
}

/// Latch or hold.
#[component]
fn ModeButton(mode: PlayMode, on: bool) -> Element {
    let ui = use_context::<Ui>();
    let label = match mode {
        PlayMode::Latch => "Latch",
        PlayMode::Hold => "Hold",
    };
    rsx! {
        button {
            class: if on { "on" } else { "" },
            title: if mode == PlayMode::Latch { "A press toggles the pad" } else { "The pad is armed while pressed" },
            onclick: move |_| play(&ui, |desk, _| {
                desk.set_mode(mode);
                None
            }),
            "{label}"
        }
    }
}

/// What the focused pad stands for: its selector, its preset, its rule and the layer.
#[component]
fn FocusCard() -> Element {
    let ui = use_context::<Ui>();
    let live = ui.live.read();
    let (Some(process), Some(running)) = (live.process(), live.shown_running()) else {
        return rsx! { div { class: "focuscard" } };
    };
    let Some(desk) = Desk::following(ui.desk.read().clone(), &process.definitions().layouts) else {
        return rsx! { div { class: "focuscard" } };
    };
    let Some(pad) = desk.surface().focused() else {
        return rsx! {
            div { class: "focuscard",
                span { class: "t4", "Encoders" }
                b { "No pad has the focus" }
                span { class: "t4", "Press a pad, or move to one with the arrow keys." }
            }
        };
    };
    let layout = desk.layout();
    let selector = layout
        .selector(pad.row())
        .map_or_else(String::new, |name| selector_title(process, name));
    let preset = layout.preset(pad.column());
    let preset_title = preset.map_or_else(String::new, |name| preset_title(process, name));
    let color = preset.map_or("#9ca3af", |name| preset_color(&process.definitions().presets, name));
    let rule = desk.rule(pad.row(), pad.column(), running);
    let state = match rule {
        None => "no rule yet".to_owned(),
        Some(status) => {
            let state = match status.state {
                RuleState::Armed => "armed",
                RuleState::Disarmed => "disarmed",
                RuleState::Draft => "draft",
            };
            let kind = if status.live { "live rule" } else { "kept rule" };
            format!("{kind} · {state}")
        }
    };
    let key = ROW_KEYS.chars().nth(pad.row().index()).unwrap_or('?');
    rsx! {
        div { class: "focuscard",
            span { class: "t4", "Encoders → pad {key}{pad.column().index() + 1}" }
            b { "{selector}" }
            div { class: "row", span { class: "fault", style: "--c:{color}", "{preset_title}" } }
            span { class: "t4", "{state} · layer {pad.layer()}" }
        }
    }
}

/// The title of the selector of that name, or its name.
fn selector_title(process: &Process, name: &SelectorName) -> String {
    let title = process
        .definitions()
        .selectors
        .iter()
        .find(|selector| selector.name() == name)
        .and_then(|selector| selector.title().map(ToString::to_string));
    titled(title, name)
}

/// The title of the preset of that name, or its name.
fn preset_title(process: &Process, name: &PresetName) -> String {
    let title = process
        .definitions()
        .presets
        .iter()
        .find(|preset| preset.name() == name)
        .and_then(|preset| preset.title().map(ToString::to_string));
    titled(title, name)
}

/// One encoder: what its parameter is on the rule of the focused pad, and a turn by the wheel or the buttons.
#[component]
fn Knob(encoder: Encoder) -> Element {
    let ui = use_context::<Ui>();
    let live = ui.live.read();
    let (Some(process), Some(running)) = (live.process(), live.shown_running()) else {
        return rsx! { div { class: "knob na" } };
    };
    let presets = &process.definitions().presets;
    let desk = Desk::following(ui.desk.read().clone(), &process.definitions().layouts);
    let turns = desk.as_ref().is_some_and(|desk| desk.turns(encoder, running, presets));
    let value = desk
        .as_ref()
        .and_then(|desk| {
            let pad = desk.surface().focused()?;
            let status = desk.rule(pad.row(), pad.column(), running)?;
            encoder_value(encoder, &status.rule, presets)
        })
        .unwrap_or_else(|| "—".to_owned());
    let label = match encoder {
        Encoder::Probability => "Probability",
        Encoder::Delay => "Delay",
        Encoder::Jitter => "Jitter",
        Encoder::ErrorCode => "Error code",
        Encoder::Throttle => "Throttle",
        Encoder::Every => "Every",
        Encoder::Duration => "Duration",
        Encoder::Firings => "Firings",
    };
    let turn = {
        let ui = ui.clone();
        move |steps: i32| {
            play(&ui, |desk, running| {
                let live = ui.live.peek();
                let presets = live
                    .process()
                    .map_or_else(Vec::new, |process| process.definitions().presets.clone());
                desk.turn(encoder, Steps::new(steps), running, &presets)
            });
        }
    };
    let wheel = {
        let turn = turn.clone();
        move |event: WheelEvent| {
            event.prevent_default();
            let up = event.delta().strip_units().y < 0.0;
            turn(if up { 1 } else { -1 });
        }
    };
    let down = {
        let turn = turn.clone();
        move |_| turn(-1)
    };
    let title = format!(
        "Encoder {}: {}",
        Encoder::ALL.iter().position(|each| *each == encoder).unwrap_or(0) + 1,
        encoder.parameter()
    );
    rsx! {
        div { class: if turns { "knob" } else { "knob na" }, title: "{title}", onwheel: wheel,
            label { "{label}" }
            div { class: "dial",
                button { class: "step", disabled: !turns, onclick: down, "−" }
                span { class: if turns { "ring on" } else { "ring" } }
                button { class: "step", disabled: !turns, onclick: move |_| turn(1), "+" }
            }
            span { class: "kv", "{value}" }
        }
    }
}

/// The header of a column: the title of its preset, in the color of its lie.
#[component]
fn ColumnHeader(preset: PresetName) -> Element {
    let ui = use_context::<Ui>();
    let live = ui.live.read();
    let Some(process) = live.process() else {
        return rsx! { div { class: "colh" } };
    };
    let title = preset_title(process, &preset);
    let color = preset_color(&process.definitions().presets, &preset);
    rsx! {
        div { class: "colh", style: "--c:{color}", title: "{preset}",
            "{title}"
            small { "{preset}" }
        }
    }
}

/// The header of a row: the title of its selector, with the letter of the row.
#[component]
fn RowHeader(row: Row, selector: SelectorName, on: bool) -> Element {
    let ui = use_context::<Ui>();
    let live = ui.live.read();
    let title = live
        .process()
        .map_or_else(|| selector.to_string(), |process| selector_title(process, &selector));
    let key = ROW_KEYS.chars().nth(row.index()).unwrap_or('?');
    rsx! {
        div { class: if on { "rowh on" } else { "rowh" },
            b { span { class: "key", "{key}" } "{title}" }
            span { class: "meta", "{selector}" }
        }
    }
}

/// One pad: lit while its rule is armed, filled as high as its chance, dashed while its rule rests.
#[component]
fn PadCell(row: Row, column: Column, color: &'static str, on: bool) -> Element {
    let ui = use_context::<Ui>();
    let live = ui.live.read();
    let (Some(process), Some(running)) = (live.process(), live.shown_running()) else {
        return rsx! { div { class: "pad" } };
    };
    let Some(desk) = Desk::following(ui.desk.read().clone(), &process.definitions().layouts) else {
        return rsx! { div { class: "pad" } };
    };
    let state = desk.state(row, column, running);
    let elsewhere = desk.armed_elsewhere(row, column, running);
    let chance = desk.rule(row, column, running).and_then(|status| {
        status
            .rule
            .trigger()
            .parts()
            .map(|parts| parts.chance().unwrap_or(Chance::ALWAYS))
    });
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "a share of 100 fits a percentage"
    )]
    let fill = chance.map_or(0, |chance| (chance.fraction() * 100.0).round() as u32);
    let mut class = String::from("pad");
    match state {
        PadState::Armed => class.push_str(" on"),
        PadState::Disarmed => class.push_str(" disarmed"),
        PadState::Empty => {}
    }
    if on {
        class.push_str(" focus");
    }
    let press = {
        let ui = ui.clone();
        move |_| play(&ui, |desk, running| desk.press(row, column, running))
    };
    let release = {
        let ui = ui.clone();
        move |_| play(&ui, |desk, running| desk.release(row, column, running))
    };
    let leave = {
        let ui = ui.clone();
        move |_| play(&ui, |desk, running| desk.release(row, column, running))
    };
    rsx! {
        div {
            class,
            style: "--c:{color}",
            onpointerdown: press,
            onpointerup: release,
            onpointerleave: leave,
            div { class: "lv", style: if state == PadState::Armed { "height:{fill}%" } else { "height:0" } }
            if elsewhere { i { class: "o" } }
            span { class: "pl", "{fill}%" }
        }
    }
}

/// One layer of the layout, with how many of its pads are armed.
#[component]
fn LayerButton(layer: Layer, number: usize, armed: usize, on: bool) -> Element {
    let ui = use_context::<Ui>();
    let switch = {
        let layer = layer.clone();
        move |_| {
            play(&ui, |desk, _| {
                desk.switch_to(&layer);
                None
            });
        }
    };
    rsx! {
        button { class: if on { "scopebtn on" } else { "scopebtn" }, title: "Layer {number}", onclick: switch,
            span { class: "nm", "{layer}" }
            span { class: "n", if armed > 0 { "{armed} armed" } else { "·" } }
        }
    }
}
