//! The small pieces that every view draws: icons, the pill of an API, the badge of a lie, the state of a rule.

use dioxus::prelude::*;
use gaslightmq_control_api::kernel::ApiKey;
use gaslightmq_control_api::lie::{LieDefinition, LieKind};
use gaslightmq_control_api::rule::RuleState;

use crate::app::Ui;

/// The shapes of the icons, as the paths of a 16 by 16 box.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    Record,
    Zap,
    Grid,
    Users,
    Check,
    Close,
    Globe,
    Play,
    Pause,
    Chevron,
}

impl Shape {
    fn paths(self) -> &'static str {
        match self {
            Shape::Record => {
                r#"<circle cx="8" cy="8" r="5.5"/><circle cx="8" cy="8" r="2.2" fill="currentColor" stroke="none"/>"#
            }
            Shape::Zap => r#"<path d="M8.8 1.8 3.5 9h4l-.6 5.2L12.5 7h-4z"/>"#,
            Shape::Grid => {
                r#"<rect x="2.5" y="2.5" width="4.5" height="4.5" rx="1"/><rect x="9" y="2.5" width="4.5" height="4.5" rx="1"/><rect x="2.5" y="9" width="4.5" height="4.5" rx="1"/><rect x="9" y="9" width="4.5" height="4.5" rx="1"/>"#
            }
            Shape::Users => {
                r#"<circle cx="6" cy="5.5" r="2.5"/><path d="M1.8 13.5c.5-2.3 2.2-3.5 4.2-3.5s3.7 1.2 4.2 3.5"/><path d="M10.5 3.2a2.4 2.4 0 0 1 0 4.6M12 10.2c1.2.5 2 1.6 2.3 3.3"/>"#
            }
            Shape::Check => r#"<path d="m3.5 8.5 3 3 6-7"/>"#,
            Shape::Close => r#"<path d="m4.5 4.5 7 7M11.5 4.5l-7 7"/>"#,
            Shape::Globe => {
                r#"<circle cx="8" cy="8" r="5.5"/><path d="M2.5 8h11M8 2.5c1.6 1.6 2.3 3.5 2.3 5.5S9.6 11.9 8 13.5C6.4 11.9 5.7 10 5.7 8S6.4 4.1 8 2.5z"/>"#
            }
            Shape::Play => r#"<path d="M5.5 3.8v8.4l6.5-4.2z"/>"#,
            Shape::Pause => r#"<path d="M6 4v8M10 4v8"/>"#,
            Shape::Chevron => r#"<path d="m4.5 6.5 3.5 3.5 3.5-3.5"/>"#,
        }
    }
}

/// An icon in the color of the text around it.
#[component]
pub fn Icon(shape: Shape, #[props(default = 14)] size: u32) -> Element {
    rsx! {
        svg {
            class: "i",
            width: "{size}",
            height: "{size}",
            view_box: "0 0 16 16",
            fill: "none",
            stroke: "currentColor",
            stroke_width: "1.5",
            stroke_linecap: "round",
            stroke_linejoin: "round",
            dangerous_inner_html: shape.paths(),
        }
    }
}

/// The color of an API, by what it is for: producing, fetching, metadata, groups, transactions, or the rest.
pub fn api_color(api: ApiKey) -> &'static str {
    match api.value() {
        0 => "#4cb782",
        1 | 2 => "#4ea7fc",
        3 | 18 | 60 | 75 => "#8a8f98",
        8..=16 | 42 | 47 | 68 | 69 => "#f2c94c",
        22..=28 => "#c084fc",
        _ => "#e879a6",
    }
}

/// The color of a kind of lie.
pub fn lie_color(lie: &LieDefinition) -> &'static str {
    match lie {
        LieDefinition::Timing(timing) if timing.delay().as_millis() == 0 => "#2dd4bf",
        _ => kind_color(lie.kind()),
    }
}

/// The color of a kind of lie, without its parameters.
pub fn kind_color(kind: LieKind) -> &'static str {
    match kind {
        LieKind::Refusal => "#eb5757",
        LieKind::Timing => "#f2994a",
        LieKind::Silence => "#a78bfa",
        LieKind::HangUp => "#4ea7fc",
        _ => "#9ca3af",
    }
}

/// The name of an API in a pill of its color, with its version when it has one.
#[component]
pub fn ApiPill(api: ApiKey, version: Option<String>) -> Element {
    let name = api.to_string();
    let color = api_color(api);
    rsx! {
        span { class: "api", style: "--c:{color}",
            i {}
            "{name}"
            if let Some(version) = version {
                span { class: "v", "v{version}" }
            }
        }
    }
}

/// A badge in the color of a lie, with what it says.
#[component]
pub fn LieBadge(color: &'static str, text: String) -> Element {
    rsx! {
        span { class: "fault", style: "--c:{color}", "{text}" }
    }
}

/// The state of a rule as a small round icon: armed, disarmed or a draft.
#[component]
pub fn StateIcon(state: RuleState, #[props(default = 14)] size: u32) -> Element {
    let paths = match state {
        RuleState::Armed => {
            r##"<circle cx="7" cy="7" r="5.5" stroke="#4cb782" stroke-width="1.5"/><circle cx="7" cy="7" r="2.6" fill="#4cb782"/>"##
        }
        RuleState::Disarmed => {
            r##"<circle cx="7" cy="7" r="5.5" stroke="#8a8f98" stroke-width="1.5"/><path d="M5.6 5v4M8.4 5v4" stroke="#8a8f98" stroke-width="1.4" stroke-linecap="round"/>"##
        }
        RuleState::Draft => {
            r##"<circle cx="7" cy="7" r="5.5" stroke="#62666d" stroke-width="1.5" stroke-dasharray="2 2.2"/>"##
        }
    };
    rsx! {
        svg {
            class: "i",
            width: "{size}",
            height: "{size}",
            view_box: "0 0 14 14",
            fill: "none",
            dangerous_inner_html: paths,
        }
    }
}

/// The notices on the screen, the newest at the bottom.
#[component]
pub fn Notices() -> Element {
    let ui = use_context::<Ui>();
    let notices = ui.notices.read();
    rsx! {
        div { class: "toasts",
            for notice in notices.iter() {
                div { key: "{notice.id}", class: if notice.failed { "toast failed" } else { "toast" },
                    Icon { shape: if notice.failed { Shape::Close } else { Shape::Check } }
                    div { "{notice.text}" }
                }
            }
        }
    }
}
