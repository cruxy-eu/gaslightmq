/// Write the revision of a resource, and the counter that gives it, into the crate that calls it.
///
/// A revision says which state of something someone saw.
/// It is a number, and one counter gives all of them: every change takes the next number, from 1 on.
/// A higher revision is therefore a later change, and no number comes back while the counter lives.
///
/// A crate that gives revisions calls the macro with the two types:
///
/// ```
/// gaslightmq_resource::revision!(Revision, RevisionCounter);
///
/// let counter = RevisionCounter::new();
/// let loaded = counter.next_revision();
/// let updated = counter.next_revision();
///
/// assert_eq!(loaded, RevisionCounter::FIRST);
/// assert_eq!(loaded.number(), 1);
/// assert!(updated > loaded);
/// assert_eq!(updated.to_string(), "2");
/// assert_eq!(format!("{updated:?}"), "Revision(2)");
/// ```
///
/// A crate that only carries revisions calls it with the revision alone:
///
/// ```
/// gaslightmq_resource::revision!(Revision);
///
/// assert!(Revision::new(8) > Revision::new(7));
/// ```
///
/// The call has up to three arguments:
///
/// - The type of the revision. It is a number and checks nothing, so it has no error.
///   It compares and sorts as its number.
/// - The type of the counter, for a crate that gives revisions.
///   The counter is a handle: a clone counts on with the counter it came from, also from another thread.
/// - The word `serde`, as the last argument, for a crate whose revisions travel.
///   The revision then serializes as its number, and the calling crate depends on `serde`:
///   `gaslightmq_resource::revision!(Revision, serde);`
///
/// The documentation that the macro writes names neither this crate nor a type of it.
/// Doc comments in front of the arguments join the documentation of the revision, after its first line.
#[macro_export]
macro_rules! revision {
    (
        $(#[$meta:meta])*
        $revision:ident $(,)?
    ) => {
        /// The revision of something: which state of it someone saw.
        ///
        $(#[$meta])*
        ///
        /// A revision is a number.
        /// One counter gives the revisions of everything that a process holds, and every change takes the next
        /// number, so a higher revision is a later change, and no revision comes back while the process runs.
        /// A revision means something in the run that gave it, and in no other: the counter starts again with
        /// every run.
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $revision(u64);

        impl $revision {
            /// The revision with that number.
            pub const fn new(number: u64) -> Self {
                Self(number)
            }

            /// The number of the revision.
            pub const fn number(self) -> u64 {
                self.0
            }
        }

        impl ::core::fmt::Debug for $revision {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.debug_tuple(stringify!($revision)).field(&self.0).finish()
            }
        }

        /// Writes the number.
        impl ::core::fmt::Display for $revision {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                ::core::fmt::Display::fmt(&self.0, f)
            }
        }
    };
    (
        $(#[$meta:meta])*
        $revision:ident, serde $(,)?
    ) => {
        $crate::revision! {
            $(#[$meta])*
            ///
            /// With `serde`, a revision is its number.
            $revision
        }

        impl ::serde::Serialize for $revision {
            fn serialize<S: ::serde::Serializer>(&self, serializer: S) -> ::core::result::Result<S::Ok, S::Error> {
                serializer.serialize_u64(self.number())
            }
        }

        impl<'de> ::serde::Deserialize<'de> for $revision {
            fn deserialize<D: ::serde::Deserializer<'de>>(deserializer: D) -> ::core::result::Result<Self, D::Error> {
                <u64 as ::serde::Deserialize>::deserialize(deserializer).map(Self::new)
            }
        }
    };
    (
        $(#[$meta:meta])*
        $revision:ident, $counter:ident $(,)?
    ) => {
        $crate::revision! {
            $(#[$meta])*
            $revision
        }

        /// The counter that gives every revision of one process.
        ///
        /// Every change takes the [next number](Self::next_revision), whoever makes it and whatever it changes.
        /// The first change takes [`FIRST`](Self::FIRST), and no number comes back.
        ///
        /// The type is a handle: a clone counts on with the counter it came from, also from another thread.
        /// A process has one counter, and whoever gives a revision holds a clone of it.
        #[derive(Debug, Clone, Default)]
        pub struct $counter {
            /// The number that the last change took, or zero while no change took one.
            last: ::std::sync::Arc<::core::sync::atomic::AtomicU64>,
        }

        impl $counter {
            /// The revision of the first change of a process.
            pub const FIRST: $revision = $revision::new(1);

            /// A counter that gave no revision yet.
            pub fn new() -> Self {
                Self::default()
            }

            /// Takes the next number: the revision of one change.
            ///
            /// It is higher than every revision that the counter, or a clone of it, gave before.
            pub fn next_revision(&self) -> $revision {
                let before = self.last.fetch_add(1, ::core::sync::atomic::Ordering::Relaxed);
                $revision::new(before + 1)
            }
        }
    };
}

#[cfg(test)]
mod tests {
    crate::revision!(Revision, RevisionCounter);

    mod travelling {
        #[cfg(feature = "serde")]
        crate::revision!(Revision, serde);
    }

    #[test]
    fn the_first_change_takes_the_first_revision_and_every_change_after_it_the_next_number() {
        let counter = RevisionCounter::new();

        let numbers: Vec<u64> = (0..3).map(|_| counter.next_revision().number()).collect();

        assert_eq!(RevisionCounter::FIRST.number(), 1);
        assert_eq!(numbers, [1, 2, 3]);
    }

    #[test]
    fn a_clone_of_a_counter_counts_on_with_it() {
        let counter = RevisionCounter::new();
        let clone = counter.clone();

        let first = counter.next_revision();
        let second = clone.next_revision();
        let third = counter.next_revision();

        assert!(first < second && second < third, "{first}, {second}, {third}");
    }

    #[test]
    fn two_counters_count_apart() {
        let one_run = RevisionCounter::new();
        let another_run = RevisionCounter::new();
        one_run.next_revision();

        assert_eq!(another_run.next_revision(), RevisionCounter::FIRST);
    }

    #[test]
    fn a_later_revision_is_the_higher_one() {
        let counter = RevisionCounter::new();
        let earlier = counter.next_revision();
        let later = counter.next_revision();

        assert!(later > earlier);
        assert_eq!(later.max(earlier), later);
        assert_eq!(Revision::new(later.number()), later);
    }

    #[test]
    fn a_revision_writes_its_number() {
        let revision = RevisionCounter::new().next_revision();

        assert_eq!(revision.to_string(), "1");
        assert_eq!(format!("{revision:?}"), "Revision(1)");
    }

    #[cfg(feature = "serde")]
    #[test]
    fn a_revision_that_travels_is_its_number_in_serde() {
        use travelling::Revision;

        let revision = Revision::new(7);

        assert_eq!(serde_json::to_string(&revision).unwrap(), "7");
        assert_eq!(serde_json::from_str::<Revision>("7").unwrap(), revision);
        assert!(serde_json::from_str::<Revision>("\"7\"").is_err());
        assert!(serde_json::from_str::<Revision>("-1").is_err());
    }
}
