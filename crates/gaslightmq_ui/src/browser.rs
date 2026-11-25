//! What the UI takes from the browser: where the control plane is, and a timer.
//!
//! The UI draws in the browser alone. A native build only checks the components and runs the tests of what the UI
//! knows: it never draws, so it never needs the address of a page, and never waits.

use gaslightmq_control_api::clock::TimeSpan;
use gaslightmq_control_client::ControlPlaneAddress;

use crate::theme::ThemeChoice;

/// The control plane that serves the page: the control server serves the UI and `/api/…` on one origin.
#[cfg(target_arch = "wasm32")]
pub fn control_plane() -> Option<ControlPlaneAddress> {
    let origin = web_sys::window()?.location().origin().ok()?;
    origin.parse().ok()
}

/// The control plane on its default address, for a build that never draws.
#[cfg(not(target_arch = "wasm32"))]
pub fn control_plane() -> Option<ControlPlaneAddress> {
    "http://127.0.0.1:7092".parse().ok()
}

/// Waits for `span` on a timer of the browser.
#[cfg(target_arch = "wasm32")]
pub async fn sleep(span: TimeSpan) {
    let millis = u32::try_from(span.as_millis()).unwrap_or(u32::MAX);
    gloo_timers::future::TimeoutFuture::new(millis).await;
}

/// Waits forever: a build that never draws has no timer.
#[cfg(not(target_arch = "wasm32"))]
pub async fn sleep(span: TimeSpan) {
    let _ = span;
    futures::future::pending::<()>().await;
}

/// Where the browser keeps the theme someone picked.
const THEME_KEY: &str = "gaslightmq-theme";

/// Shows the theme that the browser keeps, follows the system while the choice is the system's, and sends the choice
/// back. A light theme is `data-theme="light"` on the page, and the dark theme is the default of the stylesheet.
fn theme_script(pick: Option<ThemeChoice>) -> String {
    let remember = pick.map_or_else(String::new, |pick| {
        format!("localStorage.setItem('{THEME_KEY}', '{}');", pick.name())
    });
    format!(
        "{remember}
        const light = matchMedia('(prefers-color-scheme: light)');
        const apply = () => {{
            const choice = localStorage.getItem('{THEME_KEY}') || 'system';
            const isLight = choice === 'light' || (choice === 'system' && light.matches);
            document.documentElement.dataset.theme = isLight ? 'light' : 'dark';
            return choice;
        }};
        if (!window.gaslightmqTheme) {{ light.addEventListener('change', apply); window.gaslightmqTheme = apply; }}
        dioxus.send(apply());"
    )
}

/// Shows the theme the browser keeps, or `pick` from here on, and returns the choice in force.
pub async fn theme(pick: Option<ThemeChoice>) -> ThemeChoice {
    let mut script = dioxus::document::eval(&theme_script(pick));
    script
        .recv::<String>()
        .await
        .map_or(ThemeChoice::System, |name| ThemeChoice::named(&name))
}

/// Waits until someone presses ⌘K, or Ctrl-K, anywhere on the page. The browser does not see the key itself.
pub async fn quick_inject_pressed(script: &mut dioxus::document::Eval) -> bool {
    script.recv::<bool>().await.unwrap_or(false)
}

/// Listens for ⌘K and Ctrl-K on the whole page, for as long as the returned script runs.
pub fn listen_for_quick_inject() -> dioxus::document::Eval {
    dioxus::document::eval(
        "window.addEventListener('keydown', (event) => {
            if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') {
                event.preventDefault();
                dioxus.send(true);
            }
        });
        await new Promise(() => {});",
    )
}
