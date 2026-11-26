//! The pads of a layout, played live: rows are selectors, columns are presets, and the bottom row picks the layer.

use dioxus::prelude::*;
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::{Action, Preset, PresetName, RuleState};
use gaslightmq_control_api::selector::SelectorName;
use gaslightmq_control_api::surface::{Column, Encoder, Layer, LayoutName, PadState, PlayMode, Row, Steps};
use gaslightmq_control_api::trigger::Chance;

use crate::app::Ui;
use crate::components::LieBadge;
use crate::components::{Icon, Shape, Tone};
use crate::style::{CRUMB, EMPTY, HEADER, KEY, SEGMENT, SEGMENT_ON, SEGMENTS};

/// The card of the focused pad, left of the encoders.
const FOCUS_CARD: &str =
    "flex w-54 min-w-0 shrink-0 flex-col justify-center gap-1.5 rounded-xl border border-edge bg-raised px-3.5 py-3";

/// A step button of an encoder.
const STEP: &str = "grid size-4 cursor-pointer place-items-center rounded text-small leading-none text-muted enabled:hover:bg-hover enabled:hover:text-strong disabled:cursor-default";
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
        div { class: CRUMB, span { class: "text-muted", Icon { shape: Shape::Grid, size: 15 } } "Pads" }
        span { class: "text-subtle", "·" }
        span { class: "text-small text-muted", "rows are selectors, columns are presets, the bottom row picks the layer" }
    };
    let Some(process) = live.process() else {
        return rsx! {
            div { class: HEADER, {header} }
            div { class: EMPTY, "Connecting to the control plane…" }
        };
    };
    let Some(running) = live.shown_running() else {
        return rsx! {
            div { class: HEADER, {header} }
            div { class: EMPTY, "No environment runs that the pads could play. Start one in the switcher at the bottom left." }
        };
    };
    let layouts = process.definitions().layouts.clone();
    let Some(desk) = Desk::following(ui.desk.read().clone(), &layouts) else {
        return rsx! {
            div { class: HEADER, {header} }
            div { class: EMPTY, "The workspace has no pad layout. A layout maps rows to selectors and columns to presets." }
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
        div { class: HEADER,
            {header}
            span { class: "flex-1" }
            if others.len() > 1 {
                div { class: SEGMENTS,
                    for (name, title) in others {
                        LayoutButton { key: "{name}", on: &name == layout.name(), name, title }
                    }
                }
            }
            div { class: SEGMENTS,
                for each in PlayMode::ALL {
                    ModeButton { key: "{each}", mode: each, on: each == mode }
                }
            }
        }
        div { class: "flex min-h-0 flex-1 justify-center overflow-auto px-6 pt-5 pb-6.5 outline-none", tabindex: "0", onkeydown: keys,
            div { class: "flex w-min flex-col gap-2",
                div { class: "flex gap-2",
                    FocusCard {}
                    div { class: "grid min-w-0 flex-1 grid-cols-8 gap-1 rounded-xl border border-edge bg-linear-to-b from-band to-panel px-1.5 pt-3 pb-2.5",
                        for encoder in Encoder::ALL {
                            Knob { key: "{encoder.name()}", encoder }
                        }
                    }
                }
                div { class: "flex gap-2",
                    div { class: "w-54 shrink-0" }
                    for (column, preset) in columns.clone() {
                        ColumnHeader { key: "{column.index()}", preset }
                    }
                }
                for (row, selector) in rows {
                    div { key: "row-{row.index()}", class: "flex gap-2",
                        RowHeader { row, selector, on: focused.as_ref().is_some_and(|pad| pad.row() == row) }
                        for (column, preset) in columns.clone() {
                            PadCell {
                                key: "{row.index()}-{column.index()}",
                                row,
                                column,
                                tone: preset_tone(&presets, &preset),
                                on: focused.as_ref().is_some_and(|pad| pad.row() == row && pad.column() == column),
                            }
                        }
                    }
                }
                div { class: "flex gap-2",
                    div { class: "flex w-54 shrink-0 flex-col justify-center px-2.5 text-caption text-subtle",
                        span { class: "font-medium text-body", "Layer" }
                        "who the grid applies to"
                    }
                    div { class: "flex flex-wrap gap-2",
                        for (index, (layer, armed)) in layer_buttons.into_iter().enumerate() {
                            LayerButton { key: "{layer}", on: layer == shown_layer, number: index + 1, layer, armed }
                        }
                    }
                }
                div { class: "mt-2 flex flex-wrap items-center gap-4.5 border-t border-edge pt-3 text-caption text-subtle",
                    span { class: "inline-flex items-center gap-1.5", span { class: "inline-block size-2.5 rounded-sm bg-lie-delay shadow-md shadow-lie-delay" } "lit = armed, fill = chance" }
                    span { class: "inline-flex items-center gap-1.5", span { class: "inline-block size-2.5 rounded-sm border border-dashed border-lie-delay/60" } "dashed = rule exists, disarmed" }
                    span { class: "inline-flex items-center gap-1.5", span { class: "inline-block size-1.25 rounded-full bg-muted" } "armed on another layer" }
                    span { class: "inline-flex items-center gap-1.5", span { class: KEY, "←↑→↓" } " move " span { class: KEY, "space" } " press " span { class: KEY, "1–9" } " layer" }
                    span { "A press of an empty pad makes its live rule and arms it. A layer is a page, not a priority." }
                }
            }
        }
    }
}

/// Changes the desk with `change`, against the environment shown, and sends the intent it gives, as a request of
/// the pads.
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
        ui.send_from(RequestSurface::Pad, intent, |()| {});
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

/// The tone of the lie of a preset, for its column.
fn preset_tone(presets: &[Preset], name: &PresetName) -> Tone {
    match presets.iter().find(|preset| preset.name() == name).map(Preset::action) {
        Some(Action::Lie(lie)) => Tone::of(lie),
        _ => Tone::Other,
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
        button { class: if on { SEGMENT_ON } else { SEGMENT }, onclick: pick, "{title}" }
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
            class: if on { SEGMENT_ON } else { SEGMENT },
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
        return rsx! { div { class: FOCUS_CARD } };
    };
    let Some(desk) = Desk::following(ui.desk.read().clone(), &process.definitions().layouts) else {
        return rsx! { div { class: FOCUS_CARD } };
    };
    let Some(pad) = desk.surface().focused() else {
        return rsx! {
            div { class: FOCUS_CARD,
                span { class: "text-tiny text-subtle", "Encoders" }
                b { "No pad has the focus" }
                span { class: "text-tiny text-subtle", "Press a pad, or move to one with the arrow keys." }
            }
        };
    };
    let layout = desk.layout();
    let selector = layout
        .selector(pad.row())
        .map_or_else(String::new, |name| selector_title(process, name));
    let preset = layout.preset(pad.column());
    let preset_title = preset.map_or_else(String::new, |name| preset_title(process, name));
    let tone = preset.map_or(Tone::Other, |name| preset_tone(&process.definitions().presets, name));
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
        div { class: FOCUS_CARD,
            span { class: "text-tiny text-subtle", "Encoders → pad {key}{pad.column().index() + 1}" }
            b { "{selector}" }
            div { class: "flex gap-1.5", LieBadge { tone, text: preset_title } }
            span { class: "text-tiny text-subtle", "{state} · layer {pad.layer()}" }
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
        return rsx! { div { class: "opacity-30" } };
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
        div { class: if turns { "flex min-w-0 select-none flex-col items-center gap-1" } else { "flex min-w-0 select-none flex-col items-center gap-1 opacity-30" }, title: "{title}", onwheel: wheel,
            label { class: "whitespace-nowrap text-micro text-muted", "{label}" }
            div { class: "flex items-center gap-0.5",
                button { class: STEP, disabled: !turns, onclick: down, "−" }
                span { class: if turns { "size-6.5 cursor-ns-resize rounded-full border-3 border-accent border-b-track bg-popover" } else { "size-6.5 rounded-full border-3 border-track bg-popover" } }
                button { class: STEP, disabled: !turns, onclick: move |_| turn(1), "+" }
            }
            span { class: "max-w-16.5 truncate whitespace-nowrap font-mono text-micro font-medium text-strong", "{value}" }
        }
    }
}

/// The header of a column: the title of its preset, in the color of its lie.
#[component]
fn ColumnHeader(preset: PresetName) -> Element {
    let ui = use_context::<Ui>();
    let live = ui.live.read();
    let Some(process) = live.process() else {
        return rsx! { div { class: "w-17 shrink-0" } };
    };
    let title = preset_title(process, &preset);
    let tone = preset_tone(&process.definitions().presets, &preset);
    rsx! {
        div { class: "flex w-17 min-w-0 shrink-0 flex-col justify-end overflow-hidden px-0.5 pt-2 pb-0.5 text-tiny font-medium leading-tight text-body", title: "{preset}",
            i { class: "mb-1.75 block h-0.75 w-3.5 rounded-xs {tone.fill()}" }
            "{title}"
            small { class: "truncate font-mono text-micro font-normal text-subtle", "{preset}" }
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
        div { class: if on { "flex h-13.5 w-54 min-w-0 shrink-0 flex-col justify-center gap-1 rounded-lg bg-selected px-2.5" } else { "flex h-13.5 w-54 min-w-0 shrink-0 flex-col justify-center gap-1 rounded-lg px-2.5" },
            b { class: "flex items-center gap-1.75 truncate text-compact font-medium text-strong",
                span { class: "w-3.5 shrink-0 font-mono text-micro font-medium text-subtle", "{key}" }
                "{title}"
            }
            span { class: "truncate pl-5.25 font-mono text-micro text-subtle", "{selector}" }
        }
    }
}

/// One pad: lit while its rule is armed, filled as high as its chance, dashed while its rule rests.
#[component]
fn PadCell(row: Row, column: Column, tone: Tone, on: bool) -> Element {
    let ui = use_context::<Ui>();
    let live = ui.live.read();
    let (Some(process), Some(running)) = (live.process(), live.shown_running()) else {
        return rsx! { div { class: "h-13.5 w-17 shrink-0" } };
    };
    let Some(desk) = Desk::following(ui.desk.read().clone(), &process.definitions().layouts) else {
        return rsx! { div { class: "h-13.5 w-17 shrink-0" } };
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
    let base = if on {
        "relative h-13.5 w-17 shrink-0 cursor-pointer touch-none select-none overflow-hidden rounded-lg border ring-2 ring-strong/85 ring-offset-2 ring-offset-panel transition active:scale-96"
    } else {
        "relative h-13.5 w-17 shrink-0 cursor-pointer touch-none select-none overflow-hidden rounded-lg border transition active:scale-96"
    };
    let class = format!("{base} {}", tone.pad(state));
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
            onpointerdown: press,
            onpointerup: release,
            onpointerleave: leave,
            if state == PadState::Armed {
                div { class: "absolute inset-x-0 bottom-0 bg-linear-to-b from-strong/20 to-transparent {tone.fill()}", style: "height:{fill}%" }
            }
            if elsewhere { i { class: "absolute top-1.5 right-1.5 size-1.25 rounded-full bg-strong/70" } }
            if state == PadState::Armed {
                span { class: "absolute bottom-1.25 left-1.75 font-mono text-micro font-semibold text-strong", "{fill}%" }
            }
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
        button { class: if on { "flex h-9 min-w-17 cursor-pointer flex-col items-center justify-center overflow-hidden whitespace-nowrap rounded-lg border border-accent/70 bg-accent/12 px-2 text-tiny leading-tight text-strong" } else { "flex h-9 min-w-17 cursor-pointer flex-col items-center justify-center overflow-hidden whitespace-nowrap rounded-lg border border-edge-strong bg-raised px-2 text-tiny leading-tight text-muted hover:text-strong" },
            title: "Layer {number}",
            onclick: switch,
            span { "{layer}" }
            span { class: "font-mono text-micro text-subtle", if armed > 0 { "{armed} armed" } else { "·" } }
        }
    }
}
