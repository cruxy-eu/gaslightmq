//! The revision that a change left, as an entry holds it.

gaslightmq_resource::revision! {
    /// An entry of a journal holds the revision that its change left, when the change has one:
    /// arming a rule leaves one, and firing it by hand leaves none.
    /// The revision orders the entries of every environment of one run, because one counter gives all of them.
    ///
    /// ```
    /// use gaslightmq_journal::prelude::*;
    ///
    /// let armed = Revision::new(7);
    /// let disarmed = Revision::new(8);
    /// assert!(disarmed > armed);
    /// assert_eq!(disarmed.number(), 8);
    /// assert_eq!(disarmed.to_string(), "8");
    /// ```
    ///
    /// With the `serde` feature, a revision serializes as its number.
    Revision
}

#[cfg(feature = "serde")]
impl serde::Serialize for Revision {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(self.number())
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for Revision {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        <u64 as serde::Deserialize>::deserialize(deserializer).map(Self::new)
    }
}
