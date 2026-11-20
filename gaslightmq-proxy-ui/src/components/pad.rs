//! The three fault pads of a grid row: ERROR, DELAY and BLACKHOLE.

use dioxus::prelude::*;
use gaslightmq_proxy_api::{DelayPad, ErrorCodeInfo, ErrorPad, Pad, Percent, error_name};

use super::knob::Knob;
use crate::state::Ctl;

const MAX_DELAY_MS: f64 = 30_000.0;

/// A toggle pad plus its knobs. `flash` is the tick `seq` in which this
/// pad fired, if it did in the latest tick.
#[component]
fn PadCell(
    kind: &'static str,
    label: &'static str,
    on: bool,
    #[props(default)] disabled: bool,
    flash: Option<u64>,
    on_toggle: EventHandler<()>,
    children: Element,
) -> Element {
    rsx! {
        div {
            class: "pad-cell {kind}",
            class: if on { "on" },
            class: if disabled { "disabled" },
            button {
                class: "pad",
                disabled,
                title: if disabled { "{label}: not supported for this API" } else { "{label}: toggle" },
                onclick: move |_| on_toggle.call(()),
                // Keyed on the tick, so the animation restarts every time it fires.
                for seq in flash.into_iter().filter(|_| on) {
                    span { key: "{seq}", class: "pad-flash" }
                }
                span { class: "pad-label", "{label}" }
            }
            div { class: "knobs", {children} }
        }
    }
}

#[component]
fn ProbabilityKnob(value: Percent, disabled: bool, on_change: EventHandler<Percent>) -> Element {
    rsx! {
        Knob {
            norm: f64::from(value) / 100.0,
            default: 1.0,
            steps: Some(101),
            caption: "PROB",
            disabled,
            format: |n: f64| format!("{}%", to_percent(n)),
            on_change: move |n: f64| on_change.call(to_percent(n)),
        }
    }
}

fn to_percent(norm: f64) -> Percent {
    (norm * 100.0).round().clamp(0.0, 100.0) as Percent
}

#[component]
pub fn ErrorPadCell(
    api_key: i16,
    pad: ErrorPad,
    codes: Vec<ErrorCodeInfo>,
    supported: bool,
    flash: Option<u64>,
) -> Element {
    let ctl = use_context::<Ctl>();
    let steps = codes.len().max(1) as u32;
    let index = codes.iter().position(|c| c.code == pad.code).unwrap_or(0);
    let default_index = codes.iter().position(|c| c.code == -1).unwrap_or(0);
    let to_norm = move |i: usize| if steps > 1 { i as f64 / f64::from(steps - 1) } else { 0.0 };
    let to_index = move |n: f64| (n * f64::from(steps.saturating_sub(1))).round() as usize;
    let label_codes = codes.clone();
    let code = pad.code;

    rsx! {
        PadCell {
            kind: "error",
            label: "ERROR",
            on: pad.enabled && supported,
            disabled: !supported,
            flash,
            on_toggle: move |_| ctl.update_row(api_key, |r| r.error.enabled = !r.error.enabled),
            ProbabilityKnob {
                value: pad.probability,
                disabled: !supported,
                on_change: move |p| ctl.update_row(api_key, |r| r.error.probability = p),
            }
            Knob {
                norm: to_norm(index),
                default: to_norm(default_index),
                steps: Some(steps),
                caption: "CODE",
                disabled: !supported,
                format: move |n: f64| match label_codes.get(to_index(n)) {
                    Some(c) => format!("{} {}", c.code, c.name),
                    None if label_codes.is_empty() => "—".to_string(),
                    None => format!("{code} {}", error_name(code).unwrap_or("?")),
                },
                on_change: move |n: f64| {
                    if let Some(c) = codes.get(to_index(n)) {
                        let code = c.code;
                        ctl.update_row(api_key, |r| r.error.code = code);
                    }
                },
            }
        }
    }
}

#[component]
pub fn DelayPadCell(api_key: i16, pad: DelayPad, flash: Option<u64>) -> Element {
    let ctl = use_context::<Ctl>();
    rsx! {
        PadCell {
            kind: "delay",
            label: "DELAY",
            on: pad.enabled,
            flash,
            on_toggle: move |_| ctl.update_row(api_key, |r| r.delay.enabled = !r.delay.enabled),
            ProbabilityKnob {
                value: pad.probability,
                disabled: false,
                on_change: move |p| ctl.update_row(api_key, |r| r.delay.probability = p),
            }
            Knob {
                norm: delay_to_norm(pad.ms),
                default: delay_to_norm(DelayPad::default().ms),
                caption: "MS",
                format: |n: f64| format_ms(norm_to_delay(n)),
                on_change: move |n: f64| {
                    let ms = norm_to_delay(n);
                    ctl.update_row(api_key, |r| r.delay.ms = ms);
                },
            }
        }
    }
}

#[component]
pub fn BlackholePadCell(api_key: i16, pad: Pad, flash: Option<u64>) -> Element {
    let ctl = use_context::<Ctl>();
    rsx! {
        PadCell {
            kind: "blackhole",
            // Wraps onto two lines inside the pad.
            label: "BLACK HOLE",
            on: pad.enabled,
            flash,
            on_toggle: move |_| {
                ctl.update_row(api_key, |r| r.blackhole.enabled = !r.blackhole.enabled)
            },
            ProbabilityKnob {
                value: pad.probability,
                disabled: false,
                on_change: move |p| ctl.update_row(api_key, |r| r.blackhole.probability = p),
            }
        }
    }
}

/// Log scale: `0.0` is 0 ms, `1.0` is 30 s.
fn norm_to_delay(norm: f64) -> u64 {
    let ms = (MAX_DELAY_MS + 1.0).powf(norm.clamp(0.0, 1.0)) - 1.0;
    // Round to two significant-ish digits so values read cleanly.
    let step = match ms {
        m if m < 100.0 => 1.0,
        m if m < 1_000.0 => 10.0,
        m if m < 10_000.0 => 100.0,
        _ => 500.0,
    };
    ((ms / step).round() * step).clamp(0.0, MAX_DELAY_MS) as u64
}

fn delay_to_norm(ms: u64) -> f64 {
    ((ms as f64).min(MAX_DELAY_MS) + 1.0).ln() / (MAX_DELAY_MS + 1.0).ln()
}

fn format_ms(ms: u64) -> String {
    if ms < 1_000 {
        format!("{ms} ms")
    } else {
        format!("{:.1} s", ms as f64 / 1000.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delay_scale_round_trips_at_the_ends() {
        assert_eq!(norm_to_delay(0.0), 0);
        assert_eq!(norm_to_delay(1.0), 30_000);
        assert_eq!(norm_to_delay(delay_to_norm(500)), 500);
        assert_eq!(norm_to_delay(delay_to_norm(2_000)), 2_000);
    }

    #[test]
    fn percent_rounds() {
        assert_eq!(to_percent(0.255), 26);
        assert_eq!(to_percent(1.2), 100);
    }
}
