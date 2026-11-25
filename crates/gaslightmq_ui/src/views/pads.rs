//! The pads of a layout, played live.

use dioxus::prelude::*;

/// The pad grid of a layout.
#[component]
pub fn Pads() -> Element {
    rsx! {
        div { class: "hdr", div { class: "crumb", "Pads" } }
        div { class: "empty", "The pads arrive with a later slice." }
    }
}
