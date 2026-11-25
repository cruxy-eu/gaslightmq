//! The rules of the environment shown, with the inspector of one.

use dioxus::prelude::*;

/// Every rule of the environment shown.
#[component]
pub fn Rules() -> Element {
    rsx! {
        div { class: "hdr", div { class: "crumb", "Rules" } }
        div { class: "empty", "The rules arrive with the next slice." }
    }
}
