//! The layout of the text of one file: where each definition is, which holds a comment, and the text after a change.
//!
//! Nothing here knows a kind of definition. It reads the structure of the text, and it edits the text in place:
//! a change replaces the text of one definition, and leaves every other byte of the file as it was.

use std::ops::Range;

use serde_saphyr::granit_parser::{Event, Parser, StructureStyle};

/// The text of one file, as the store read it: what its document holds, and where.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Document {
    /// No node: an empty file, a file of comments, or a null.
    Nothing,
    /// A list at the top of the file. Each item is a definition.
    List(List),
    /// One node at the top of the file, which is no list.
    One(Item),
}

/// A list at the top of a file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct List {
    /// Whether the list is written `- item`, or `[item, item]`.
    block: bool,
    /// Where the list is: the brackets of a list in flow style.
    span: Range<usize>,
    items: Vec<Item>,
    /// Whether an empty line stands between the first two items, so that a new item gets one too.
    spaced: bool,
}

/// Where one node of a file is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Item {
    /// What a change replaces: from the `-` of a list item, or from the start of the node,
    /// to the end of its last line. The end of that line holds no other node, only a comment.
    range: Range<usize>,
    /// The node alone, which reads as one definition.
    node: Range<usize>,
    /// The line and the column where the node starts, counted from zero.
    line: usize,
    column: usize,
    /// The column of the start of `range`, which every line of the text that replaces the item keeps.
    indent: usize,
    /// Whether a comment starts inside `range`.
    commented: bool,
}

/// Why the text of a file does not hold the layout of a file of definitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Unreadable {
    pub(crate) reason: String,
}

impl Document {
    /// The layout of a text.
    ///
    /// # Errors
    ///
    /// When the text is no YAML, or it holds more than one document.
    pub(crate) fn parse(text: &str) -> Result<Self, Unreadable> {
        let mut layout = Layout::new(text);
        for next in Parser::new_from_str(text) {
            let (event, span) = next.map_err(|error| Unreadable {
                reason: error.to_string(),
            })?;
            let start = span.start.byte_offset().unwrap_or(text.len());
            let end = span.end.byte_offset().unwrap_or(start);
            layout.take(&event, start..end)?;
        }
        Ok(layout.finish())
    }

    /// The items that a list holds, or the one node of a file.
    pub(crate) fn items(&self) -> &[Item] {
        match self {
            Self::Nothing => &[],
            Self::List(list) => &list.items,
            Self::One(item) => std::slice::from_ref(item),
        }
    }

    /// The text with `written` in the place of the item of this index.
    ///
    /// `written` is the written form of a definition, a mapping that starts at column zero.
    /// It keeps the indentation of the item, and the item stays an item of its list.
    ///
    /// # Errors
    ///
    /// When the item is in a list in flow style, which a change does not edit.
    pub(crate) fn replace(&self, text: &str, index: usize, written: &str) -> Result<String, InFlow> {
        let (item, in_list) = match self {
            Self::List(list) if !list.block => return Err(InFlow),
            Self::List(list) => (&list.items[index], true),
            Self::One(item) => (item, false),
            Self::Nothing => return Ok(text.to_owned()),
        };
        let mut after = String::with_capacity(text.len() + written.len());
        after.push_str(&text[..item.range.start]);
        after.push_str(&placed(written, in_list, item.indent));
        after.push_str(&text[item.range.end..]);
        Ok(after)
    }

    /// The text with `written` as a new item, after the last item of its list.
    ///
    /// A text that holds nothing yet becomes a list in block style.
    ///
    /// # Errors
    ///
    /// When the list is in flow style and holds items, which a change does not edit.
    pub(crate) fn append(&self, text: &str, written: &str) -> Result<String, InFlow> {
        match self {
            Self::Nothing | Self::One(_) => {
                let mut after = text.to_owned();
                if !after.is_empty() && !after.ends_with('\n') {
                    after.push('\n');
                }
                after.push_str(&placed(written, true, 0));
                after.push('\n');
                Ok(after)
            }
            Self::List(list) if !list.block && !list.items.is_empty() => Err(InFlow),
            Self::List(list) => match list.items.last() {
                // `[]`: the brackets give way to the first item.
                None => {
                    let indent = column_of(text, list.span.start);
                    let mut after = String::with_capacity(text.len() + written.len());
                    after.push_str(&text[..list.span.start]);
                    after.push_str(&placed(written, true, indent));
                    after.push_str(&text[list.span.end..]);
                    Ok(after)
                }
                Some(previous) => {
                    let mut after = String::with_capacity(text.len() + written.len());
                    after.push_str(&text[..previous.range.end]);
                    after.push('\n');
                    if list.spaced {
                        after.push('\n');
                    }
                    after.push_str(&" ".repeat(previous.indent));
                    after.push_str(&placed(written, true, previous.indent));
                    after.push_str(&text[previous.range.end..]);
                    Ok(after)
                }
            },
        }
    }

    /// The text without the item of this index: its lines go, and so does one empty line around them.
    ///
    /// # Errors
    ///
    /// When the item is in a list in flow style, which a change does not edit.
    pub(crate) fn remove(&self, text: &str, index: usize) -> Result<String, InFlow> {
        let item = match self {
            Self::List(list) if !list.block => return Err(InFlow),
            Self::List(list) => &list.items[index],
            Self::One(_) | Self::Nothing => return Ok(String::new()),
        };
        let line_start = line_start(text, item.range.start);
        let start = if text[line_start..item.range.start].trim().is_empty() {
            line_start
        } else {
            item.range.start
        };
        let end = match text[item.range.end..].find('\n') {
            Some(newline) => item.range.end + newline + 1,
            None => text.len(),
        };

        let before = &text[..start];
        let mut after = &text[end..];
        let blank_before = before.is_empty() || before.ends_with("\n\n");
        if blank_before && after.starts_with('\n') {
            after = &after[1..];
        }
        let mut text = String::with_capacity(before.len() + after.len());
        text.push_str(before);
        text.push_str(after);
        if after.is_empty() {
            let kept = text.trim_end_matches('\n').len();
            text.truncate(kept);
            if kept > 0 {
                text.push('\n');
            }
        }
        Ok(text)
    }
}

impl Item {
    /// The node alone, as a text that reads as the node did in its file, with the same lines and columns.
    pub(crate) fn alone(&self, text: &str) -> String {
        let node = &text[self.node.clone()];
        let mut alone = String::with_capacity(self.line + self.column + node.len());
        alone.push_str(&"\n".repeat(self.line));
        alone.push_str(&" ".repeat(self.column));
        alone.push_str(node);
        alone
    }

    /// Whether a comment starts inside the text of the item, which a change of it would lose.
    pub(crate) fn commented(&self) -> bool {
        self.commented
    }
}

/// A change in a list in flow style, `[item, item]`, which a change does not edit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InFlow;

/// The written form of a definition as the text of an item: `- ` before a list item, and the indentation of its place.
fn placed(written: &str, in_list: bool, indent: usize) -> String {
    let pad = " ".repeat(indent);
    let mut placed = String::with_capacity(written.len());
    for (index, line) in written.trim_end_matches('\n').split('\n').enumerate() {
        if index == 0 {
            if in_list {
                placed.push_str("- ");
            }
        } else {
            placed.push('\n');
            if !line.is_empty() {
                placed.push_str(&pad);
                if in_list {
                    placed.push_str("  ");
                }
            }
        }
        placed.push_str(line);
    }
    placed
}

/// The byte where the line of `at` starts.
fn line_start(text: &str, at: usize) -> usize {
    text[..at].rfind('\n').map_or(0, |newline| newline + 1)
}

/// The column of `at`, in characters.
fn column_of(text: &str, at: usize) -> usize {
    text[line_start(text, at)..at].chars().count()
}

/// The layout of a text, as its events arrive.
struct Layout<'t> {
    text: &'t str,
    documents: usize,
    /// The collections that are open, each with whether it is in block style.
    open: Vec<bool>,
    top: Top,
    /// The item whose events arrive now.
    item: Option<Building>,
    comments: Vec<usize>,
}

/// What the top of the document is, so far.
enum Top {
    Nothing,
    List {
        block: bool,
        start: usize,
        end: usize,
        items: Vec<Building>,
    },
    One(Building),
}

/// An item whose events arrive.
#[derive(Clone)]
struct Building {
    /// Where its first event starts.
    node_start: usize,
    /// Where the last of its events that hold text ends.
    end: usize,
    /// How many collections are open around it: one inside a list, none at the top of the file.
    depth: usize,
}

impl<'t> Layout<'t> {
    fn new(text: &'t str) -> Self {
        Self {
            text,
            documents: 0,
            open: Vec::new(),
            top: Top::Nothing,
            item: None,
            comments: Vec::new(),
        }
    }

    fn take(&mut self, event: &Event<'_>, span: Range<usize>) -> Result<(), Unreadable> {
        match event {
            Event::DocumentStart(..) => {
                self.documents += 1;
                if self.documents > 1 {
                    return Err(Unreadable {
                        reason: "the file holds more than one document".to_owned(),
                    });
                }
            }
            Event::Comment(..) => self.comments.push(span.start),
            Event::SequenceStart(style, ..) | Event::MappingStart(style, ..) => {
                let block = *style == StructureStyle::Block;
                let is_list = matches!(event, Event::SequenceStart(..));
                if self.open.is_empty() && is_list {
                    self.top = Top::List {
                        block,
                        start: span.start,
                        end: span.end,
                        items: Vec::new(),
                    };
                } else {
                    self.begin(span.clone());
                }
                self.open.push(block);
                self.reach(&span);
            }
            Event::SequenceEnd | Event::MappingEnd => {
                self.reach(&span);
                self.open.pop();
                if self.open.is_empty()
                    && let Top::List { end, .. } = &mut self.top
                {
                    *end = (*end).max(span.end);
                }
                self.close();
            }
            Event::Scalar(value, style, ..) => {
                let empty = span.is_empty();
                let null = self.open.is_empty()
                    && *style == serde_saphyr::granit_parser::ScalarStyle::Plain
                    && matches!(value.as_ref(), "" | "~" | "null" | "Null" | "NULL");
                if !null {
                    self.begin(span.clone());
                    if !empty {
                        self.reach(&span);
                    }
                    self.close();
                }
            }
            Event::Alias(..) => {
                self.begin(span.clone());
                self.reach(&span);
                self.close();
            }
            _ => {}
        }
        Ok(())
    }

    /// A node starts: at the top of the file, or as an item of the list there.
    fn begin(&mut self, span: Range<usize>) {
        let depth = self.open.len();
        let is_item = match self.top {
            Top::List { .. } => depth == 1,
            Top::Nothing => depth == 0,
            Top::One(_) => false,
        };
        if is_item && self.item.is_none() {
            self.item = Some(Building {
                node_start: span.start,
                end: span.start,
                depth,
            });
        }
    }

    /// An event of the item holds text up to the end of `span`.
    fn reach(&mut self, span: &Range<usize>) {
        if let Some(item) = &mut self.item
            && !span.is_empty()
        {
            item.end = item.end.max(span.end);
        }
    }

    /// A node ends: the item ends with it, when it is the node of the item.
    fn close(&mut self) {
        let ends = self.item.as_ref().is_some_and(|item| self.open.len() == item.depth);
        if !ends {
            return;
        }
        let Some(item) = self.item.take() else {
            return;
        };
        match &mut self.top {
            Top::List { items, .. } => items.push(item),
            Top::Nothing => self.top = Top::One(item),
            Top::One(_) => {}
        }
    }

    fn finish(self) -> Document {
        match &self.top {
            Top::Nothing => Document::Nothing,
            Top::One(item) => Document::One(self.item_at(item, true, false)),
            Top::List {
                block,
                start,
                end,
                items,
            } => {
                let items: Vec<Item> = items.iter().map(|item| self.item_at(item, *block, *block)).collect();
                let spaced = match items.as_slice() {
                    [first, second, ..] => has_empty_line(&self.text[first.range.end..second.range.start]),
                    _ => false,
                };
                Document::List(List {
                    block: *block,
                    span: *start..*end,
                    items,
                    spaced,
                })
            }
        }
    }

    /// Where an item is in the text.
    ///
    /// The end of an item in block style reaches the end of its last line: what is left there is a `:` of a key
    /// without a value, or a comment. An item of a list in block style starts at its `-`.
    fn item_at(&self, building: &Building, block: bool, in_block_list: bool) -> Item {
        let text = self.text;
        let node_start = building.node_start;
        let content_end = node_start + text[node_start..building.end].trim_end().len();
        let end = if block {
            let line_end = text[content_end..]
                .find('\n')
                .map_or(text.len(), |newline| content_end + newline);
            if text[..line_end].ends_with('\r') {
                line_end - 1
            } else {
                line_end
            }
        } else {
            content_end
        };
        let start = if in_block_list {
            let before = text[..node_start].trim_end();
            if before.ends_with('-') {
                before.len() - 1
            } else {
                node_start
            }
        } else {
            node_start
        };
        Item {
            range: start..end,
            node: node_start..end,
            line: text[..node_start].matches('\n').count(),
            column: column_of(text, node_start),
            indent: column_of(text, start),
            commented: self.comments.iter().any(|comment| (start..end).contains(comment)),
        }
    }
}

/// Whether a text between two items holds a line with nothing on it.
fn has_empty_line(between: &str) -> bool {
    let lines: Vec<&str> = between.split('\n').collect();
    lines.len() > 2 && lines[1..lines.len() - 1].iter().any(|line| line.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    const SELECTORS: &str = "\
# Which calls.

- name: order-producers
  match:
    api: Produce   # writes
    topic: orders

- name: order-fetches
  match:
    api: Fetch
";

    fn parsed(text: &str) -> Document {
        Document::parse(text).expect("the text reads")
    }

    #[test]
    fn a_list_knows_where_each_item_is_and_which_holds_a_comment() {
        let document = parsed(SELECTORS);
        let items = document.items();

        assert_eq!(items.len(), 2);
        assert_eq!(
            &SELECTORS[items[0].range.clone()],
            "- name: order-producers\n  match:\n    api: Produce   # writes\n    topic: orders"
        );
        assert!(items[0].commented());
        assert_eq!(
            &SELECTORS[items[1].range.clone()],
            "- name: order-fetches\n  match:\n    api: Fetch"
        );
        assert!(!items[1].commented());
    }

    #[test]
    fn a_replaced_item_leaves_every_other_byte() {
        let written = "name: order-fetches\nmatch:\n  api: Fetch\n  topic: orders\n";
        let after = parsed(SELECTORS)
            .replace(SELECTORS, 1, written)
            .expect("a list in block style");

        assert_eq!(
            after,
            SELECTORS.replace(
                "- name: order-fetches\n  match:\n    api: Fetch\n",
                "- name: order-fetches\n  match:\n    api: Fetch\n    topic: orders\n"
            )
        );
    }

    #[test]
    fn a_new_item_follows_the_last_one_with_the_space_between_the_others() {
        let after = parsed(SELECTORS)
            .append(SELECTORS, "name: metadata\nmatch:\n  api: Metadata\n")
            .expect("a list in block style");

        assert_eq!(
            after,
            format!("{SELECTORS}\n- name: metadata\n  match:\n    api: Metadata\n")
        );
    }

    #[test]
    fn a_removed_item_takes_one_empty_line_with_it() {
        let document = parsed(SELECTORS);

        assert_eq!(
            document.remove(SELECTORS, 0).expect("a list in block style"),
            "# Which calls.\n\n- name: order-fetches\n  match:\n    api: Fetch\n"
        );
        assert_eq!(
            document.remove(SELECTORS, 1).expect("a list in block style"),
            "# Which calls.\n\n- name: order-producers\n  match:\n    api: Produce   # writes\n    topic: orders\n"
        );
    }

    #[test]
    fn a_key_without_a_value_and_a_comment_at_the_end_of_the_line_belong_to_the_item() {
        let text = "- name: a\n  title:   # none yet\n- name: b\n";
        let document = parsed(text);

        assert_eq!(
            &text[document.items()[0].range.clone()],
            "- name: a\n  title:   # none yet"
        );
        assert!(document.items()[0].commented());
    }

    #[test]
    fn a_node_alone_keeps_its_lines_and_columns() {
        let document = parsed(SELECTORS);

        assert_eq!(
            document.items()[1].alone(SELECTORS),
            format!("{}  name: order-fetches\n  match:\n    api: Fetch", "\n".repeat(7))
        );
    }

    #[test]
    fn a_file_of_comments_holds_nothing_and_takes_a_first_item() {
        let text = "# Nothing yet.\n";
        let document = parsed(text);

        assert_eq!(document, Document::Nothing);
        assert_eq!(
            document.append(text, "name: a\n").expect("nothing to edit"),
            "# Nothing yet.\n- name: a\n"
        );
    }

    #[test]
    fn an_empty_flow_list_gives_way_to_the_first_item() {
        let text = "[]\n";

        assert_eq!(
            parsed(text).append(text, "name: a\n").expect("an empty list"),
            "- name: a\n"
        );
    }

    #[test]
    fn a_list_in_flow_style_is_not_edited() {
        let text = "[{name: a}, {name: b}]\n";
        let document = parsed(text);

        assert_eq!(document.items().len(), 2);
        assert_eq!(document.replace(text, 0, "name: a\n"), Err(InFlow));
        assert_eq!(document.append(text, "name: c\n"), Err(InFlow));
        assert_eq!(document.remove(text, 0), Err(InFlow));
    }

    #[test]
    fn one_node_is_replaced_after_the_comments_above_it() {
        let text = "# The shop.\nname: shop\n# The broker.\nbroker:\n  world: shop\n";
        let document = parsed(text);

        assert!(document.items()[0].commented());
        assert_eq!(
            document
                .replace(text, 0, "name: shop\nbroker:\n  world: orders\n")
                .expect("one node"),
            "# The shop.\nname: shop\nbroker:\n  world: orders\n"
        );
    }

    #[test]
    fn a_second_document_does_not_read() {
        let error = Document::parse("name: a\n---\nname: b\n").expect_err("two documents");

        assert_eq!(error.reason, "the file holds more than one document");
    }
}
