//! The check that the UI styles with the tokens of its theme, and with nothing else.
//!
//! The theme of the UI names every colour by its role, once, with a dark and a light value: see [`ui_theme`].
//! A component names those roles as Tailwind classes, such as `bg-panel` or `text-muted`, so the same markup serves
//! both themes. The check reads every string literal in the Rust files of the UI crate, inside macros too,
//! and fails on what bypasses the tokens:
//!
//! - a raw colour: `#5e6ad2`, `rgb(`, `rgba(`, `hsl(`, `hsla(`, `oklch(`, `oklab(`, `lch(` or `lab(`;
//! - an arbitrary value of Tailwind: `bg-[#000]`, `w-[216px]`, `bg-(--brand)` or `[--c:red]`;
//! - a class that names a colour of Tailwind's palette rather than a role: `bg-zinc-900`, `text-white`;
//! - a `dark:` variant, because a component carries no theme of its own.
//!
//! What the check does not catch:
//!
//! - A class built from pieces at runtime, which Tailwind cannot see either.
//! - A style that reaches the page another way than a string of the crate, such as a file that the crate reads.
//! - A string that only looks like a class, and is text: a sentence with `text-white` in it fails all the same.
//! - Doc comments, which hold no markup.
//!
//! [`ui_theme`]: crate::ui_theme

use std::fmt;
use std::path::PathBuf;

use proc_macro2::{Delimiter, TokenStream, TokenTree};

use crate::source::{self, SourceError};
use crate::workspace::Workspace;

/// The crate of the UI.
pub const UI_CRATE: &str = "gaslightmq_ui";

/// The functions of CSS that write a colour by its numbers.
const COLOUR_FUNCTIONS: [&str; 9] = [
    "rgb(", "rgba(", "hsl(", "hsla(", "oklch(", "oklab(", "lch(", "lab(", "hwb(",
];

/// The utilities of Tailwind that take a colour.
const COLOUR_UTILITIES: [&str; 22] = [
    "bg",
    "text",
    "border",
    "border-t",
    "border-r",
    "border-b",
    "border-l",
    "border-x",
    "border-y",
    "ring",
    "ring-offset",
    "outline",
    "divide",
    "shadow",
    "inset-shadow",
    "drop-shadow",
    "fill",
    "stroke",
    "from",
    "via",
    "to",
    "decoration",
];

/// The colours of Tailwind's own palette.
const PALETTE: [&str; 22] = [
    "slate", "gray", "zinc", "neutral", "stone", "red", "orange", "amber", "yellow", "lime", "green", "emerald",
    "teal", "cyan", "sky", "blue", "indigo", "violet", "purple", "fuchsia", "pink", "rose",
];

/// Every place in the UI crate that styles without the tokens of the theme.
pub fn violations(workspace: &Workspace) -> Result<Vec<RawStyle>, SourceError> {
    let Some(member) = workspace.members.iter().find(|member| member.name == UI_CRATE) else {
        return Ok(Vec::new());
    };
    let mut violations = Vec::new();
    for (path, text) in source::texts_of(&workspace.root, member)? {
        violations.extend(in_text(&path, &text)?);
    }
    Ok(violations)
}

/// The violations in the text of one Rust file.
fn in_text(path: &std::path::Path, text: &str) -> Result<Vec<RawStyle>, SourceError> {
    let tokens: TokenStream = text
        .parse()
        .map_err(|error: proc_macro2::LexError| SourceError::NotRust {
            path: path.to_path_buf(),
            line: error.span().start().line,
            reason: error.to_string(),
        })?;
    let mut violations = Vec::new();
    walk(tokens, &mut |line, value| {
        violations.extend(found_in(value).into_iter().map(|found| RawStyle {
            path: path.to_path_buf(),
            line,
            found,
        }));
    });
    Ok(violations)
}

/// Calls `visit` with the line and the value of every string literal, inside groups and macros too.
/// A doc comment is an attribute `#[doc = "…"]` by now, and holds no markup, so the walk passes over it.
fn walk(tokens: TokenStream, visit: &mut impl FnMut(usize, &str)) {
    for token in tokens {
        match token {
            TokenTree::Group(group) => {
                let is_doc = group.delimiter() == Delimiter::Bracket
                    && matches!(group.stream().into_iter().next(), Some(TokenTree::Ident(ident)) if ident == "doc");
                if !is_doc {
                    walk(group.stream(), visit);
                }
            }
            TokenTree::Literal(literal) => {
                if let Ok(string) = syn::parse_str::<syn::LitStr>(&literal.to_string()) {
                    visit(literal.span().start().line, &string.value());
                }
            }
            TokenTree::Ident(_) | TokenTree::Punct(_) => {}
        }
    }
}

/// What a string holds that bypasses the tokens.
fn found_in(value: &str) -> Vec<Found> {
    let mut found = Vec::new();
    if let Some(colour) = raw_colour(value) {
        found.push(Found::RawColour(colour));
    }
    for class in value.split_whitespace() {
        let utility = class.rsplit(':').next().unwrap_or(class);
        if class.starts_with("dark:") || class.contains(":dark:") {
            found.push(Found::DarkVariant(class.to_owned()));
        } else if is_arbitrary(class) {
            found.push(Found::ArbitraryValue(class.to_owned()));
        } else if names_palette(utility) {
            found.push(Found::PaletteColour(class.to_owned()));
        }
    }
    found
}

/// The first raw colour in a string: a hexadecimal colour, or a colour function of CSS.
fn raw_colour(value: &str) -> Option<String> {
    let lower = value.to_ascii_lowercase();
    if let Some(function) = COLOUR_FUNCTIONS.iter().find(|function| lower.contains(*function)) {
        return Some((*function).to_owned());
    }
    let bytes = value.as_bytes();
    for (at, _) in value.match_indices('#') {
        let digits = bytes[at + 1..]
            .iter()
            .take_while(|byte| byte.is_ascii_hexdigit())
            .count();
        let after = bytes.get(at + 1 + digits);
        let ends = after.is_none_or(|byte| !(byte.is_ascii_alphanumeric() || *byte == b'_' || *byte == b'-'));
        if matches!(digits, 3 | 4 | 6 | 8) && ends {
            return Some(value[at..=at + digits].to_owned());
        }
    }
    None
}

/// Whether a class takes an arbitrary value or property: `w-[216px]`, `bg-(--brand)`, `[--c:red]`.
fn is_arbitrary(class: &str) -> bool {
    let property = class
        .split_once('[')
        .and_then(|(_, inside)| inside.split_once(']'))
        .is_some_and(|(inside, _)| inside.contains(':'));
    class.contains("-[") || class.contains("-(") || property
}

/// Whether a utility names a colour of Tailwind's palette, such as `bg-zinc-900` or `text-white`.
fn names_palette(utility: &str) -> bool {
    let utility = utility.trim_start_matches(['!', '-']);
    let utility = utility.split('/').next().unwrap_or(utility);
    COLOUR_UTILITIES.iter().any(|prefix| {
        let Some(colour) = utility.strip_prefix(prefix).and_then(|rest| rest.strip_prefix('-')) else {
            return false;
        };
        if matches!(colour, "white" | "black") {
            return true;
        }
        colour.split_once('-').is_some_and(|(name, shade)| {
            PALETTE.contains(&name) && !shade.is_empty() && shade.bytes().all(|byte| byte.is_ascii_digit())
        })
    })
}

/// What styles without the tokens.
#[derive(Debug, PartialEq)]
pub enum Found {
    /// A colour written by its numbers.
    RawColour(String),
    /// A class with an arbitrary value or property.
    ArbitraryValue(String),
    /// A class that names a colour of Tailwind's palette.
    PaletteColour(String),
    /// A class for the dark theme alone.
    DarkVariant(String),
}

/// A string of the UI crate that styles without the tokens of the theme.
#[derive(Debug, PartialEq)]
pub struct RawStyle {
    path: PathBuf,
    line: usize,
    found: Found,
}

impl fmt::Display for RawStyle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}: ", self.path.display(), self.line)?;
        match &self.found {
            Found::RawColour(colour) => write!(
                f,
                "`{colour}` is a raw colour; name a colour token of the theme instead, such as `text-muted`"
            ),
            Found::ArbitraryValue(class) => write!(
                f,
                "`{class}` is an arbitrary value; use a token or a step of the theme's scale instead"
            ),
            Found::PaletteColour(class) => write!(
                f,
                "`{class}` names a colour of Tailwind's palette; name the role instead, such as `bg-panel`"
            ),
            Found::DarkVariant(class) => write!(
                f,
                "`{class}` is a `dark:` variant; a component names tokens, and the theme gives each a dark value"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    /// The violations in one file of the UI crate, as the lines the check prints.
    fn lines(text: &str) -> Vec<String> {
        in_text(Path::new("crates/gaslightmq_ui/src/views/pads.rs"), text)
            .unwrap()
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    #[test]
    fn classes_that_name_tokens_pass() {
        let text = r#"
            /// The colour `#5e6ad2` of the mockup, in a doc comment, is no markup.
            const ROW: &str = "flex h-8 items-center gap-2 border-b border-edge/50 bg-accent/12 hover:bg-hover";
            fn pad() { rsx! { div { class: "text-muted size-6.5 rounded-lg", "[{partitions}]" } } }
        "#;

        assert_eq!(lines(text), [""; 0]);
    }

    #[test]
    fn a_raw_colour_fails_inside_a_macro_too() {
        let text = r##"fn icon() { rsx! { svg { stroke: "#4cb782" } } }
            const SHADE: &str = "oklch(0.2 0 0)";"##;

        assert_eq!(
            lines(text),
            [
                "crates/gaslightmq_ui/src/views/pads.rs:1: `#4cb782` is a raw colour; \
                 name a colour token of the theme instead, such as `text-muted`",
                "crates/gaslightmq_ui/src/views/pads.rs:2: `oklch(` is a raw colour; \
                 name a colour token of the theme instead, such as `text-muted`",
            ]
        );
    }

    #[test]
    fn an_arbitrary_value_fails() {
        let text = r#"const GRID: &str = "grid w-[216px] hover:bg-(--brand) [--c:red]";"#;

        assert_eq!(lines(text).len(), 3, "{:?}", lines(text));
        assert!(lines(text)[0].contains("`w-[216px]` is an arbitrary value"));
    }

    #[test]
    fn a_colour_of_the_palette_fails_and_a_role_passes() {
        let text = r#"const CARD: &str = "bg-zinc-900 hover:text-white/80 text-strong bg-lie-delay/30";"#;

        assert_eq!(
            lines(text),
            [
                "crates/gaslightmq_ui/src/views/pads.rs:1: `bg-zinc-900` names a colour of Tailwind's palette; \
                 name the role instead, such as `bg-panel`",
                "crates/gaslightmq_ui/src/views/pads.rs:1: `hover:text-white/80` names a colour of Tailwind's \
                 palette; name the role instead, such as `bg-panel`",
            ]
        );
    }

    #[test]
    fn a_dark_variant_fails() {
        let text = r#"const BUTTON: &str = "bg-raised dark:bg-panel md:dark:text-strong";"#;

        assert_eq!(lines(text).len(), 2);
        assert!(lines(text)[0].contains("`dark:bg-panel` is a `dark:` variant"));
    }
}
