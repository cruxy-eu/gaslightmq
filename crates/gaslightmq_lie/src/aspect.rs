use std::fmt;
use std::str::FromStr;

use crate::LieKind;

/// What a rewrite changes about its target.
///
/// Rewrites merge per target and aspect.
/// A stale high watermark and dropped records on the same partition both apply, because they change two aspects.
/// Two rewrites of the same aspect of the same target conflict.
///
/// Every aspect belongs to one [`LieKind`], and only the kinds that rewrite have aspects.
///
/// ```
/// use gaslightmq_lie::prelude::*;
///
/// assert_eq!(Aspect::HighWatermark.kind(), LieKind::Progress);
/// assert_eq!(Aspect::HighWatermark.to_string(), "high-watermark");
/// assert_eq!(LieKind::Data.aspects(), [Aspect::Records]);
/// assert_eq!(LieKind::Refusal.aspects(), []);
/// ```
///
/// A new rewrite can bring a new aspect, so a `match` on an aspect needs a wildcard arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum Aspect {
    /// Who leads a partition.
    Leader,
    /// Which brokers the cluster has, and where they are.
    Brokers,
    /// Which broker is the controller.
    Controller,
    /// The records of a partition.
    Records,
    /// The high watermark of a partition.
    HighWatermark,
    /// The log start offset of a partition.
    LogStartOffset,
    /// The offset a group committed on a partition.
    CommittedOffset,
    /// The settings of a topic or a broker.
    Configs,
}

impl Aspect {
    /// Every aspect, kind by kind in the order of [`LieKind::ALL`].
    pub const ALL: [Self; 8] = [
        Self::Leader,
        Self::Brokers,
        Self::Controller,
        Self::Records,
        Self::HighWatermark,
        Self::LogStartOffset,
        Self::CommittedOffset,
        Self::Configs,
    ];

    /// The name a file uses, such as `high-watermark`.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Leader => "leader",
            Self::Brokers => "brokers",
            Self::Controller => "controller",
            Self::Records => "records",
            Self::HighWatermark => "high-watermark",
            Self::LogStartOffset => "log-start-offset",
            Self::CommittedOffset => "committed-offset",
            Self::Configs => "configs",
        }
    }

    /// The kind of lie that rewrites this aspect.
    pub const fn kind(self) -> LieKind {
        match self {
            Self::Leader | Self::Brokers | Self::Controller => LieKind::Topology,
            Self::Records => LieKind::Data,
            Self::HighWatermark | Self::LogStartOffset | Self::CommittedOffset => LieKind::Progress,
            Self::Configs => LieKind::Configuration,
        }
    }
}

/// Writes the name a file uses, such as `high-watermark`.
impl fmt::Display for Aspect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(self.name())
    }
}

/// Reads the name a file uses, as [`Display`](fmt::Display) writes it.
impl FromStr for Aspect {
    type Err = AspectError;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|aspect| aspect.name() == name)
            .ok_or_else(|| AspectError::Unknown { name: name.to_owned() })
    }
}

/// Why a text is not an [`Aspect`].
///
/// ```
/// use gaslightmq_lie::prelude::*;
///
/// let error = "watermark".parse::<Aspect>().unwrap_err();
/// assert_eq!(error, AspectError::Unknown { name: "watermark".to_owned() });
/// assert_eq!(error.to_string(), "no aspect is called \"watermark\"");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AspectError {
    /// No aspect has this name.
    #[error("no aspect is called {name:?}")]
    Unknown { name: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_aspects_come_kind_by_kind() {
        let names: Vec<&str> = Aspect::ALL.into_iter().map(Aspect::name).collect();
        assert_eq!(
            names,
            [
                "leader",
                "brokers",
                "controller",
                "records",
                "high-watermark",
                "log-start-offset",
                "committed-offset",
                "configs",
            ]
        );

        let mut sorted = Aspect::ALL;
        sorted.sort();
        assert_eq!(sorted, Aspect::ALL);

        let by_kind: Vec<Aspect> = LieKind::ALL
            .into_iter()
            .flat_map(|kind| kind.aspects().iter().copied())
            .collect();
        assert_eq!(by_kind, Aspect::ALL);
    }

    #[test]
    fn an_aspect_belongs_to_the_kind_that_lists_it() {
        for aspect in Aspect::ALL {
            assert!(aspect.kind().aspects().contains(&aspect), "{aspect}");
        }
        for kind in LieKind::ALL {
            for aspect in kind.aspects() {
                assert_eq!(aspect.kind(), kind, "{aspect}");
            }
        }
    }

    #[test]
    fn an_aspect_reads_back_from_the_name_it_writes() {
        for aspect in Aspect::ALL {
            assert_eq!(aspect.to_string().parse(), Ok(aspect));
        }
    }

    #[test]
    fn a_name_that_no_aspect_has_is_an_error() {
        for name in ["Leader", "high watermark", "watermark", ""] {
            assert_eq!(
                name.parse::<Aspect>(),
                Err(AspectError::Unknown { name: name.to_owned() }),
            );
        }
    }

    #[test]
    fn an_aspect_error_says_what_is_wrong() {
        let error = AspectError::Unknown {
            name: "watermark".into(),
        };

        assert_eq!(error.to_string(), "no aspect is called \"watermark\"");
    }
}
