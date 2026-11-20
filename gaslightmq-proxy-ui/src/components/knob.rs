//! A Push-style endless-looking encoder: an SVG arc dragged vertically.

use std::f64::consts::PI;

use dioxus::prelude::*;

use crate::platform;

/// Pixels of vertical drag for the full range.
const DRAG_RANGE_PX: f64 = 160.0;
/// Minimum time between sends while dragging.
const THROTTLE_MS: u32 = 50;
/// How long to keep showing the local value after release, so stale
/// echoes of intermediate values don't make the knob twitch.
const SETTLE_MS: u32 = 400;
/// Half of the 270° sweep, in radians.
const HALF_SWEEP: f64 = 0.75 * PI;

const SIZE: f64 = 36.0;
const CENTER: f64 = SIZE / 2.0;
const RADIUS: f64 = 14.0;

/// A knob over a normalized `0.0..=1.0` value. Callers map to and from
/// their own units.
#[component]
pub fn Knob(
    /// Current value from the model.
    norm: f64,
    /// Value restored by a double click.
    default: f64,
    /// Snap to this many evenly spaced positions.
    #[props(default)]
    steps: Option<u32>,
    /// Label shown under the knob.
    format: Callback<f64, String>,
    /// Throttled while dragging; always called with the final value.
    on_change: EventHandler<f64>,
    caption: &'static str,
    #[props(default)] disabled: bool,
) -> Element {
    let mut drag = use_signal(|| None::<Drag>);
    let throttle = Throttle {
        local: use_signal(|| None),
        sent: use_signal(|| None),
        flushing: use_signal(|| false),
        on_change,
    };

    let shown = throttle.local.read().unwrap_or(norm);
    let label = format.call(shown);

    let end = move |_| {
        if drag.peek().is_none() {
            return;
        }
        drag.set(None);
        throttle.finish(drag);
    };

    rsx! {
        div {
            class: "knob",
            class: if disabled { "disabled" },
            class: if drag.read().is_some() { "dragging" },
            title: "{caption}: {label} (drag, double-click to reset)",
            onpointerdown: move |e: PointerEvent| {
                if disabled {
                    return;
                }
                e.prevent_default();
                capture_pointer(&e);
                let y = e.client_coordinates().y;
                drag.set(Some(Drag { start_y: y, start_norm: shown }));
                throttle.begin(shown);
            },
            onpointermove: move |e: PointerEvent| {
                let Some(d) = *drag.peek() else { return };
                let dy = d.start_y - e.client_coordinates().y;
                let value = snap((d.start_norm + dy / DRAG_RANGE_PX).clamp(0.0, 1.0), steps);
                throttle.update(value);
            },
            onpointerup: end,
            onpointercancel: end,
            onlostpointercapture: end,
            ondoubleclick: move |_| {
                if disabled {
                    return;
                }
                throttle.begin(shown);
                throttle.update(snap(default, steps));
                throttle.finish(drag);
            },
            svg {
                class: "knob-svg",
                view_box: "0 0 {SIZE} {SIZE}",
                width: "{SIZE}",
                height: "{SIZE}",
                path { class: "knob-track", d: arc(0.0, 1.0) }
                if shown > 0.001 {
                    path { class: "knob-value", d: arc(0.0, shown) }
                }
                circle { class: "knob-cap", cx: "{CENTER}", cy: "{CENTER}", r: "{RADIUS - 5.0}" }
                Needle { norm: shown }
            }
            div { class: "knob-label", "{label}" }
            div { class: "knob-caption", "{caption}" }
        }
    }
}

#[component]
fn Needle(norm: f64) -> Element {
    let (x1, y1) = point(norm, RADIUS - 11.0);
    let (x2, y2) = point(norm, RADIUS - 5.0);
    rsx! {
        line { class: "knob-needle", x1: "{x1}", y1: "{y1}", x2: "{x2}", y2: "{y2}" }
    }
}

#[derive(Clone, Copy, PartialEq)]
struct Drag {
    start_y: f64,
    start_norm: f64,
}

/// Leading-edge throttle with a trailing flush.
#[derive(Clone, Copy)]
struct Throttle {
    /// Value shown while dragging or settling.
    local: Signal<Option<f64>>,
    /// Last value handed to `on_change`.
    sent: Signal<Option<f64>>,
    /// A flush loop is running.
    flushing: Signal<bool>,
    on_change: EventHandler<f64>,
}

impl Throttle {
    fn begin(mut self, current: f64) {
        self.local.set(Some(current));
        self.sent.set(Some(current));
    }

    fn update(mut self, value: f64) {
        if *self.local.peek() == Some(value) {
            return;
        }
        self.local.set(Some(value));
        if *self.flushing.peek() {
            return;
        }
        self.flushing.set(true);
        self.send(value);
        spawn(async move {
            loop {
                platform::sleep_ms(THROTTLE_MS).await;
                if !self.flush() {
                    break;
                }
            }
            self.flushing.set(false);
        });
    }

    /// Send the pending value; `false` if there was nothing to send.
    fn flush(self) -> bool {
        let pending = *self.local.peek();
        match pending {
            Some(value) if pending != *self.sent.peek() => {
                self.send(value);
                true
            }
            _ => false,
        }
    }

    fn send(mut self, value: f64) {
        self.sent.set(Some(value));
        self.on_change.call(value);
    }

    /// Send the final value now and drop the local override once settled.
    fn finish(mut self, drag: Signal<Option<Drag>>) {
        self.flush();
        spawn(async move {
            platform::sleep_ms(SETTLE_MS).await;
            if drag.peek().is_none() {
                self.local.set(None);
            }
        });
    }
}

fn snap(value: f64, steps: Option<u32>) -> f64 {
    match steps {
        Some(n) if n > 1 => {
            let last = f64::from(n - 1);
            (value * last).round() / last
        }
        _ => value,
    }
}

/// Point on the knob circle at `norm` along the sweep.
fn point(norm: f64, radius: f64) -> (f64, f64) {
    let angle = -HALF_SWEEP + norm * 2.0 * HALF_SWEEP;
    (CENTER + radius * angle.sin(), CENTER - radius * angle.cos())
}

/// SVG path for the arc between two normalized positions.
fn arc(from: f64, to: f64) -> String {
    let (x0, y0) = point(from, RADIUS);
    let (x1, y1) = point(to, RADIUS);
    let large = i32::from((to - from) * 2.0 * HALF_SWEEP > PI);
    format!("M {x0:.2} {y0:.2} A {RADIUS} {RADIUS} 0 {large} 1 {x1:.2} {y1:.2}")
}

/// Keep receiving move events when the pointer leaves the knob.
#[cfg(target_arch = "wasm32")]
fn capture_pointer(e: &PointerEvent) {
    use web_sys::wasm_bindgen::JsCast;

    let data = e.data();
    let Some(event) = data.downcast::<web_sys::PointerEvent>() else {
        return;
    };
    // Events are delegated, so `current_target` is the root. Capture on the
    // knob itself: the arc under the pointer may be removed mid-drag.
    let knob = event
        .target()
        .and_then(|t| t.dyn_into::<web_sys::Element>().ok())
        .and_then(|el| el.closest(".knob").ok().flatten());
    if let Some(knob) = knob {
        let _ = knob.set_pointer_capture(event.pointer_id());
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn capture_pointer(_e: &PointerEvent) {}
