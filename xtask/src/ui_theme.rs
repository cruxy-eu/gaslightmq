//! The check that the theme of the UI gives every colour token a dark and a light value, both in OKLCH.
//!
//! The theme lives in one file, [`THEME`]. Its `@theme` block names every colour token by its role, with the dark
//! value, and its block for `[data-theme="light"]` gives each the light value. The components only name the tokens:
//! see [`ui_tokens`]. The check fails on:
//!
//! - a raw colour anywhere in the file that is no OKLCH: a hexadecimal colour, `rgb(`, `rgba(`, `hsl(` or `hsla(`;
//! - a colour token whose value is not `oklch(…)`, in either theme;
//! - a colour token that one theme has and the other lacks;
//! - a file without the `@theme` block or without the light block.
//!
//! `--color-*: initial`, which clears the palette of Tailwind, is no token.
//!
//! What the check does not catch:
//!
//! - A token that is no colour, such as a font or a size: it has one value for both themes.
//! - A value in OKLCH that reads badly, such as text that has too little contrast. That takes a person.
//! - A colour that a rule outside the two blocks sets, which the first rule of the list only catches when it is no
//!   OKLCH.
//!
//! [`ui_tokens`]: crate::ui_tokens

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;

/// The theme file, relative to the workspace root.
pub const THEME: &str = "crates/gaslightmq_ui/tailwind.css";

/// The selector of the block that holds the light values.
const LIGHT: &str = "[data-theme=\"light\"]";

/// The prefix of a colour token.
const COLOUR: &str = "--color-";

/// The functions of CSS that write a colour, other than OKLCH.
const OTHER_FUNCTIONS: [&str; 4] = ["rgb(", "rgba(", "hsl(", "hsla("];

/// Everything wrong with the theme. A workspace without the UI crate has no theme, and passes.
pub fn violations(root: &Path) -> Result<Vec<ThemeViolation>, ThemeError> {
    let path = root.join(THEME);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let text = std::fs::read_to_string(&path).map_err(|error| ThemeError::Unreadable {
        reason: error.to_string(),
    })?;
    Ok(in_text(&text))
}

/// What is wrong with the text of a theme file.
fn in_text(text: &str) -> Vec<ThemeViolation> {
    let text = without_comments(text);
    let mut violations = raw_colours(&text);
    let Some(dark) = block(&text, "@theme") else {
        violations.push(ThemeViolation::NoBlock { block: "@theme" });
        return violations;
    };
    let Some(light) = block(&text, LIGHT) else {
        violations.push(ThemeViolation::NoBlock { block: LIGHT });
        return violations;
    };
    let dark = colours(&text, dark);
    let light = colours(&text, light);
    for (theme, own, other) in [("dark", &dark, &light), ("light", &light, &dark)] {
        for (token, (line, value)) in own {
            if !value.starts_with("oklch(") {
                violations.push(ThemeViolation::NotOklch {
                    line: *line,
                    token: token.clone(),
                    theme,
                });
            }
            if !other.contains_key(token) {
                violations.push(ThemeViolation::Missing {
                    line: *line,
                    token: token.clone(),
                    theme: if theme == "dark" { "light" } else { "dark" },
                });
            }
        }
    }
    violations.sort_by_key(ThemeViolation::line);
    violations
}

/// The text with every comment blanked out, so that the lines stay where they were.
fn without_comments(text: &str) -> String {
    let mut kept = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find("/*") {
        kept.push_str(&rest[..start]);
        let end = rest[start..].find("*/").map_or(rest.len(), |end| start + end + 2);
        kept.extend(
            rest[start..end]
                .chars()
                .map(|char| if char == '\n' { '\n' } else { ' ' }),
        );
        rest = &rest[end..];
    }
    kept.push_str(rest);
    kept
}

/// The line of the byte at `offset`, counted from one.
fn line_of(text: &str, offset: usize) -> usize {
    text[..offset].matches('\n').count() + 1
}

/// Every colour of the text that is no OKLCH.
fn raw_colours(text: &str) -> Vec<ThemeViolation> {
    let mut found = Vec::new();
    let lower = text.to_ascii_lowercase();
    for function in OTHER_FUNCTIONS {
        for (offset, _) in lower.match_indices(function) {
            let starts_a_word = offset == 0 || !lower.as_bytes()[offset - 1].is_ascii_alphanumeric();
            if starts_a_word {
                found.push(ThemeViolation::RawColour {
                    line: line_of(text, offset),
                    colour: function.to_owned(),
                });
            }
        }
    }
    let bytes = text.as_bytes();
    for (offset, _) in text.match_indices('#') {
        let digits = bytes[offset + 1..]
            .iter()
            .take_while(|byte| byte.is_ascii_hexdigit())
            .count();
        let ends = bytes
            .get(offset + 1 + digits)
            .is_none_or(|byte| !(byte.is_ascii_alphanumeric() || *byte == b'_' || *byte == b'-'));
        if matches!(digits, 3 | 4 | 6 | 8) && ends {
            found.push(ThemeViolation::RawColour {
                line: line_of(text, offset),
                colour: text[offset..=offset + digits].to_owned(),
            });
        }
    }
    found
}

/// Where the body of the first block after `selector` starts and ends, as offsets into the text.
fn block(text: &str, selector: &str) -> Option<(usize, usize)> {
    let at = text.find(selector)?;
    let open = at + text[at..].find('{')? + 1;
    let close = open + text[open..].find('}')?;
    Some((open, close))
}

/// The colour tokens that a block declares, each with the line of its declaration and its value.
fn colours(text: &str, (start, end): (usize, usize)) -> BTreeMap<String, (usize, String)> {
    let mut found = BTreeMap::new();
    let mut offset = start;
    for declaration in text[start..end].split(';') {
        let leading = declaration.len() - declaration.trim_start().len();
        let line = line_of(text, offset + leading);
        offset += declaration.len() + 1;
        let Some((name, value)) = declaration.split_once(':') else {
            continue;
        };
        let (name, value) = (name.trim(), value.trim());
        if name.starts_with(COLOUR) && name != "--color-*" {
            found.insert(name.to_owned(), (line, value.to_owned()));
        }
    }
    found
}

/// A way in which the theme falls short.
#[derive(Debug, PartialEq)]
pub enum ThemeViolation {
    /// A colour of the file that is no OKLCH.
    RawColour { line: usize, colour: String },
    /// A colour token whose value in one theme is no OKLCH.
    NotOklch {
        line: usize,
        token: String,
        theme: &'static str,
    },
    /// A colour token that one theme lacks. `theme` is the one that lacks it.
    Missing {
        line: usize,
        token: String,
        theme: &'static str,
    },
    /// The file lacks a block.
    NoBlock { block: &'static str },
}

impl ThemeViolation {
    fn line(&self) -> usize {
        match self {
            ThemeViolation::RawColour { line, .. }
            | ThemeViolation::NotOklch { line, .. }
            | ThemeViolation::Missing { line, .. } => *line,
            ThemeViolation::NoBlock { .. } => 0,
        }
    }
}

impl fmt::Display for ThemeViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ThemeViolation::RawColour { line, colour } => {
                write!(
                    f,
                    "{THEME}:{line}: `{colour}` is no OKLCH; write the colour as `oklch(L C H)`"
                )
            }
            ThemeViolation::NotOklch { line, token, theme } => write!(
                f,
                "{THEME}:{line}: the {theme} value of `{token}` is no OKLCH; write it as `oklch(L C H)`"
            ),
            ThemeViolation::Missing { line, token, theme } => write!(
                f,
                "{THEME}:{line}: `{token}` has no {theme} value; every colour token has a dark and a light one"
            ),
            ThemeViolation::NoBlock { block } => write!(
                f,
                "{THEME}: the file has no `{block}` block; it holds the dark values in `@theme` and the light \
                 values in `{LIGHT}`"
            ),
        }
    }
}

/// Why the theme could not be read.
#[derive(Debug, thiserror::Error)]
pub enum ThemeError {
    #[error("{THEME} cannot be read: {reason}")]
    Unreadable { reason: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lines(text: &str) -> Vec<String> {
        in_text(text).iter().map(ToString::to_string).collect()
    }

    #[test]
    fn a_theme_with_every_token_in_oklch_in_both_themes_passes() {
        let text = "@import \"tailwindcss\";
            /* The roles, with the dark values. #000 in a comment is no colour. */
            @theme {
              --color-*: initial;
              --color-panel: oklch(0.173 0.004 264.5);
              --color-hover: oklch(1 0 0 / 0.035);
              --font-mono: \"JetBrains Mono\", monospace;
            }
            :root[data-theme=\"light\"] {
              --color-panel: oklch(0.99 0.002 264);
              --color-hover: oklch(0 0 0 / 0.035);
            }";

        assert_eq!(lines(text), [""; 0]);
    }

    #[test]
    fn a_hexadecimal_or_rgb_value_fails() {
        let text = "@theme {
              --color-panel: #0f1012;
            }
            :root[data-theme=\"light\"] {
              --color-panel: rgb(250 250 250);
            }";

        assert_eq!(
            lines(text),
            [
                "crates/gaslightmq_ui/tailwind.css:2: `#0f1012` is no OKLCH; write the colour as `oklch(L C H)`",
                "crates/gaslightmq_ui/tailwind.css:2: the dark value of `--color-panel` is no OKLCH; \
                 write it as `oklch(L C H)`",
                "crates/gaslightmq_ui/tailwind.css:5: `rgb(` is no OKLCH; write the colour as `oklch(L C H)`",
                "crates/gaslightmq_ui/tailwind.css:5: the light value of `--color-panel` is no OKLCH; \
                 write it as `oklch(L C H)`",
            ]
        );
    }

    #[test]
    fn a_token_that_one_theme_lacks_fails() {
        let text = "@theme {
              --color-panel: oklch(0.173 0.004 264.5);
              --color-danger: oklch(0.653 0.183 23.7);
            }
            :root[data-theme=\"light\"] {
              --color-panel: oklch(0.99 0.002 264);
              --color-glow: oklch(0.9 0.1 90);
            }";

        assert_eq!(
            lines(text),
            [
                "crates/gaslightmq_ui/tailwind.css:3: `--color-danger` has no light value; \
                 every colour token has a dark and a light one",
                "crates/gaslightmq_ui/tailwind.css:7: `--color-glow` has no dark value; \
                 every colour token has a dark and a light one",
            ]
        );
    }

    #[test]
    fn a_theme_without_its_light_block_fails() {
        let text = "@theme { --color-panel: oklch(0.173 0.004 264.5); }";

        assert_eq!(
            lines(text),
            [
                "crates/gaslightmq_ui/tailwind.css: the file has no `[data-theme=\"light\"]` block; \
                 it holds the dark values in `@theme` and the light values in `[data-theme=\"light\"]`"
            ]
        );
    }
}
