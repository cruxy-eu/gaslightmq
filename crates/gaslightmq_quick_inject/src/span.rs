use std::fmt;
use std::ops::Range;

/// Where a part of a sentence stands: from a byte of its text up to, and not including, another.
///
/// Both ends are positions between characters, so a span cuts the text it came from without a panic.
/// A surface marks what the parser could not read with it.
///
/// ```
/// use gaslightmq_quick_inject::prelude::*;
///
/// let typed = "delay prodcue 2s";
/// let sentence = QuickInject::default().read(typed, &KnownNames::new());
///
/// let span = sentence.unread()[0].span();
/// assert_eq!((span.start(), span.end()), (6, 13));
/// assert_eq!(&typed[span.range()], "prodcue");
/// assert_eq!(span.to_string(), "6..13");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Span {
    start: usize,
    end: usize,
}

impl Span {
    /// The span from `start` up to `end`, which the parser took from the text it cut.
    pub(crate) const fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    /// The span that runs from the start of this one to the end of `other`.
    pub(crate) const fn up_to(self, other: Span) -> Self {
        Self::new(self.start, other.end)
    }

    /// The byte the part starts at.
    pub const fn start(self) -> usize {
        self.start
    }

    /// The byte after the last one of the part.
    pub const fn end(self) -> usize {
        self.end
    }

    /// How many bytes the part has.
    pub const fn len(self) -> usize {
        self.end - self.start
    }

    /// Whether the part has no bytes: a position, such as the end of a sentence.
    pub const fn is_empty(self) -> bool {
        self.start == self.end
    }

    /// The span as the range that cuts the part out of the text of the sentence.
    pub const fn range(self) -> Range<usize> {
        self.start..self.end
    }
}

impl From<Span> for Range<usize> {
    fn from(span: Span) -> Self {
        span.range()
    }
}

/// Writes both ends: `6..13`.
impl fmt::Display for Span {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}..{}", self.start, self.end)
    }
}
