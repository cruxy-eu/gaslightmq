use gaslightmq_field::prelude::*;
use gaslightmq_lie::prelude::*;

use crate::{Claim, FiredLie, Priority};

/// A target that a lie lost, and the lie it lost it to.
///
/// A lie that is **shadowed** lost a conflict: the other lie has a higher priority.
/// A lie that is **pre-empted** had nothing left to change: the other lie got there first.
/// A [`Fate`] says which of the two a loss is.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Loss<Id> {
    pub(crate) on: Target,
    pub(crate) to: Id,
}

impl<Id> Loss<Id> {
    /// The target the lie lost: some of its parts, or the whole call.
    pub const fn on(&self) -> &Target {
        &self.on
    }

    /// The lie that took the target.
    pub const fn to(&self) -> &Id {
        &self.to
    }
}

/// What became of a lie that fired: where it applied, and where it lost to which other lie.
///
/// A lie on several parts can meet a different fate on each.
/// A refusal of `orders.eu/3` and `orders.eu/4` can apply on one partition and be shadowed on the other.
///
/// | Status of an explanation | Here |
/// |---|---|
/// | Applied | [`applied`](Self::applied) names the target. |
/// | Shadowed | [`shadowed`](Self::shadowed) names each target and its winner. |
/// | Pre-empted | [`pre_empted`](Self::pre_empted) names each target and the lie that got there first. |
///
/// The targets of the three never overlap, and together they are the target the lie claimed.
///
/// A firing [counts](Self::counts) when the lie applied on at least one target.
///
/// ```
/// use gaslightmq_combine::prelude::*;
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_lie::prelude::*;
///
/// let [three, four]: [Part; 2] = ["orders.eu/3".parse()?, "orders.eu/4".parse()?];
/// let refusal = |parts: &[&Part]| Claim::Refusal {
///     stage: Stage::BeforeForwarding,
///     target: Target::of_parts(parts.iter().copied().cloned()).expect("a part"),
/// };
///
/// let call = RequestPass::of([
///     FiredLie::new("Leader flap", Priority::new(0), refusal(&[&three])),
///     FiredLie::new("Refuse orders", Priority::new(1), refusal(&[&three, &four])),
/// ])
/// .without_response_pass();
///
/// let fate = call.fate_of(&"Refuse orders").expect("the lie fired");
/// assert_eq!(fate.applied(), Target::of_parts([four]).as_ref());
/// assert_eq!(Some(fate.shadowed()[0].on()), Target::of_parts([three]).as_ref());
/// assert_eq!(fate.shadowed()[0].to(), &"Leader flap");
/// assert!(fate.pre_empted().is_empty());
///
/// // Shadowed on one partition and applied on another: one firing.
/// assert!(fate.counts());
/// # Ok::<(), FieldError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Fate<Id> {
    pub(crate) lie: FiredLie<Id>,
    pub(crate) stage: Stage,
    pub(crate) applied: Option<Target>,
    pub(crate) shadowed: Vec<Loss<Id>>,
    pub(crate) pre_empted: Vec<Loss<Id>>,
}

impl<Id> Fate<Id> {
    /// The lie this is the fate of.
    pub const fn lie(&self) -> &FiredLie<Id> {
        &self.lie
    }

    /// Who the lie is.
    pub const fn id(&self) -> &Id {
        self.lie.id()
    }

    /// Where the lie stands in the priority order of the call.
    pub const fn priority(&self) -> Priority {
        self.lie.priority()
    }

    /// What the lie claimed of the call.
    pub const fn claim(&self) -> &Claim {
        self.lie.claim()
    }

    /// The stage the lie acts at.
    ///
    /// It is the stage the lie names, except for a lie of the response pass, which always acts after the response.
    pub const fn stage(&self) -> Stage {
        self.stage
    }

    /// The target the lie applies to, or `None` when it applies nowhere.
    pub const fn applied(&self) -> Option<&Target> {
        self.applied.as_ref()
    }

    /// The targets on which a lie with a higher priority won the conflict, one entry per winner.
    /// The winner with the highest priority comes first.
    pub fn shadowed(&self) -> &[Loss<Id>] {
        &self.shadowed
    }

    /// The targets on which the lie had nothing left to change, one entry per lie that got there first.
    /// The one with the highest priority comes first.
    pub fn pre_empted(&self) -> &[Loss<Id>] {
        &self.pre_empted
    }

    /// Whether the firing counts: the lie applied on at least one target.
    ///
    /// A firing that counts is spent for good in a budget of firings.
    /// A firing that was shadowed or pre-empted on every target goes back to the budget.
    pub const fn counts(&self) -> bool {
        self.applied.is_some()
    }
}
