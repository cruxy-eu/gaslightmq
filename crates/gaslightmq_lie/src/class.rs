use std::fmt;
use std::str::FromStr;

/// How a lie combines with the other lies on the same call.
///
/// | Class | Lies | Combination |
/// |---|---|---|
/// | Modifier | Timing | Modifiers stack. Delays add up, and throttle takes the maximum. |
/// | Rewrite | Topology, data, progress, configuration | Rewrites merge per target and [aspect](crate::Aspect). Two rewrites of the same aspect of the same target conflict. |
/// | Refusal | Refusal | Refusals of different parts all apply. Two refusals of the same part conflict. A refusal pre-empts the rewrites of its part. |
/// | Terminal | Silence, hang-up | A terminal lie ends the whole call. Only one can apply, and it pre-empts modifiers and rewrites. |
///
/// The class is a fact about a [`LieKind`](crate::LieKind).
/// This crate only names it, and whoever combines lies decides with it.
///
/// ```
/// use gaslightmq_lie::prelude::*;
///
/// assert_eq!(LieKind::Timing.class(), CombinationClass::Modifier);
/// assert_eq!(LieKind::HangUp.class(), CombinationClass::Terminal);
/// assert_eq!(CombinationClass::Terminal.to_string(), "terminal");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CombinationClass {
    /// Changes when or how fast the answer arrives, and leaves what it says alone.
    Modifier,
    /// Changes one aspect of the answer.
    Rewrite,
    /// Replaces everything about its target with an error.
    Refusal,
    /// Ends the whole call.
    Terminal,
}

impl CombinationClass {
    /// Every class, in the order of the specification.
    pub const ALL: [Self; 4] = [Self::Modifier, Self::Rewrite, Self::Refusal, Self::Terminal];

    /// The name a file uses, such as `modifier`.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Modifier => "modifier",
            Self::Rewrite => "rewrite",
            Self::Refusal => "refusal",
            Self::Terminal => "terminal",
        }
    }
}

/// Writes the name a file uses, such as `modifier`.
impl fmt::Display for CombinationClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(self.name())
    }
}

/// Reads the name a file uses, as [`Display`](fmt::Display) writes it.
impl FromStr for CombinationClass {
    type Err = CombinationClassError;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|class| class.name() == name)
            .ok_or_else(|| CombinationClassError::Unknown { name: name.to_owned() })
    }
}

/// Why a text is not a [`CombinationClass`].
///
/// ```
/// use gaslightmq_lie::prelude::*;
///
/// let error = "rewrites".parse::<CombinationClass>().unwrap_err();
/// assert_eq!(error, CombinationClassError::Unknown { name: "rewrites".to_owned() });
/// assert_eq!(error.to_string(), "no combination class is called \"rewrites\"");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum CombinationClassError {
    /// No combination class has this name.
    #[error("no combination class is called {name:?}")]
    Unknown { name: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn there_are_four_classes_in_the_order_of_the_specification() {
        let names: Vec<&str> = CombinationClass::ALL.into_iter().map(CombinationClass::name).collect();
        assert_eq!(names, ["modifier", "rewrite", "refusal", "terminal"]);

        let mut sorted = CombinationClass::ALL;
        sorted.sort();
        assert_eq!(sorted, CombinationClass::ALL);
    }

    #[test]
    fn a_class_reads_back_from_the_name_it_writes() {
        for class in CombinationClass::ALL {
            assert_eq!(class.to_string().parse(), Ok(class));
        }
    }

    #[test]
    fn a_name_that_no_class_has_is_an_error() {
        for name in ["Modifier", "rewrites", ""] {
            assert_eq!(
                name.parse::<CombinationClass>(),
                Err(CombinationClassError::Unknown { name: name.to_owned() }),
            );
        }
    }

    #[test]
    fn a_class_error_says_what_is_wrong() {
        let error = CombinationClassError::Unknown {
            name: "rewrites".into(),
        };

        assert_eq!(error.to_string(), "no combination class is called \"rewrites\"");
    }
}
