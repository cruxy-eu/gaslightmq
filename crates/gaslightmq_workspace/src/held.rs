//! The UUIDs of the definitions that a workspace holds, in memory.

use std::collections::BTreeMap;

use crate::kind::{AnyUuid, Kind};
use crate::{DefinitionName, UuidSource};

/// The UUID of every definition that a workspace holds, under its kind and its name.
///
/// No definition and no store holds a UUID: it is live state of the process, as the armed state of a rule is.
/// The workspace gives a definition its UUID when it first holds it, keeps it for as long as the name stays,
/// and forgets it when the definition goes. A name that comes back gets a new one.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct HeldUuids {
    by_name: BTreeMap<DefinitionName, AnyUuid>,
}

impl HeldUuids {
    /// The UUID of the definition of the kind `D` and this name, while the workspace holds it.
    pub(crate) fn get<D: Kind>(&self, name: &DefinitionName) -> Option<D::Uuid> {
        self.by_name.get(name).copied().and_then(D::uuid_of)
    }

    /// Takes in the names of every definition that the stores hold now.
    ///
    /// A name that the workspace does not hold yet gets a new UUID from `uuids`, in the order of the names.
    /// A name that the stores no longer hold is a definition that went, so its UUID is forgotten.
    pub(crate) fn take_in(&mut self, names: impl IntoIterator<Item = DefinitionName>, uuids: &mut impl UuidSource) {
        let mut by_name = BTreeMap::new();
        for name in names {
            let uuid = match self.by_name.remove(&name) {
                Some(uuid) => uuid,
                None => AnyUuid::new(&name, uuids.seed()),
            };
            by_name.insert(name, uuid);
        }
        self.by_name = by_name;
    }

    /// Holds the definition of the kind `D` and this name under `uuid`, in the place of the UUID it had.
    pub(crate) fn hold<D: Kind>(&mut self, name: DefinitionName, uuid: D::Uuid) {
        self.by_name.insert(name, D::any_uuid(uuid));
    }

    /// Forgets the UUID of the definition of this name.
    pub(crate) fn forget(&mut self, name: &DefinitionName) {
        self.by_name.remove(name);
    }
}
