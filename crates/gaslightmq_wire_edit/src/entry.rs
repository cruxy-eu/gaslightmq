//! The entries of a response, and how the entries of two responses become one list.
//!
//! A response with [parts](gaslightmq_field::Part) is a tree: topics that hold partitions, and sometimes groups that hold topics.
//! Two responses to one request merge entry by entry, and that is the same for every API.
//! An API only says what each of its entries is: [`partition!`], [`topic!`], [`topic_part!`], [`group!`] or [`response!`].

use crate::body::TopicKey;

/// One entry of a response: a group, a topic or a partition. The response itself is the entry that holds the others.
pub(crate) trait Entry: Sized {
    /// Whether `other` is the entry of the same group, topic or partition.
    fn same(&self, other: &Self) -> bool;

    /// Takes what `other`, an entry of the same thing, says.
    /// An entry that holds entries merges them, and any other entry becomes `other`.
    fn absorb(&mut self, other: Self);

    /// Puts the entries it holds in the order that `pattern` has them.
    fn order_like(&mut self, pattern: &Self);

    /// Shows every part at or below this entry, with its error code.
    /// `topic` is the topic entry above this one.
    fn each_part(&self, topic: Option<TopicKey<'_>>, see: &mut dyn FnMut(TopicKey<'_>, Option<i32>, i16));
}

/// Puts the entries of `from` into `into`: an entry of both is absorbed, and another one is added at the end.
pub(crate) fn merge<T: Entry>(into: &mut Vec<T>, from: Vec<T>) {
    for entry in from {
        match into.iter_mut().find(|known| known.same(&entry)) {
            Some(known) => known.absorb(entry),
            None => into.push(entry),
        }
    }
}

/// Puts `entries` in the order that `pattern` has them, all the way down.
/// An entry that `pattern` does not have comes after the others, where it stood.
pub(crate) fn order<T: Entry>(entries: &mut [T], pattern: &[T]) {
    let place = |entry: &T| pattern.iter().position(|like| like.same(entry));
    entries.sort_by_cached_key(|entry| place(entry).unwrap_or(usize::MAX));
    for entry in entries {
        if let Some(like) = pattern.iter().find(|like| like.same(entry)) {
            entry.order_like(like);
        }
    }
}

/// Shows every part at or below `entries`.
pub(crate) fn each_part<T: Entry>(
    entries: &[T],
    topic: Option<TopicKey<'_>>,
    see: &mut dyn FnMut(TopicKey<'_>, Option<i32>, i16),
) {
    for entry in entries {
        entry.each_part(topic, see);
    }
}

/// The entry of one partition, below a topic: `partition!(Type, index_field, error_code_field)`.
macro_rules! partition {
    ($entry:ty, $index:ident, $error_code:ident) => {
        impl $crate::entry::Entry for $entry {
            fn same(&self, other: &Self) -> bool {
                self.$index == other.$index
            }

            fn absorb(&mut self, other: Self) {
                *self = other;
            }

            fn order_like(&mut self, _pattern: &Self) {}

            fn each_part(
                &self,
                topic: Option<$crate::body::TopicKey<'_>>,
                see: &mut dyn FnMut($crate::body::TopicKey<'_>, Option<i32>, i16),
            ) {
                if let Some(topic) = topic {
                    see(topic, Some(self.$index), self.$error_code);
                }
            }
        }
    };
}

/// The entry of one topic that holds partitions: `topic!(Type, |topic| TopicKey::named(&topic.name), partitions_field)`.
macro_rules! topic {
    ($entry:ty, |$topic:ident| $named:expr, $partitions:ident) => {
        impl $crate::entry::Entry for $entry {
            fn same(&self, other: &Self) -> bool {
                let one = {
                    let $topic = self;
                    $named
                };
                let other = {
                    let $topic = other;
                    $named
                };
                $crate::body::same_topic(one, other)
            }

            fn absorb(&mut self, other: Self) {
                $crate::entry::merge(&mut self.$partitions, other.$partitions);
            }

            fn order_like(&mut self, pattern: &Self) {
                $crate::entry::order(&mut self.$partitions, &pattern.$partitions);
            }

            fn each_part(
                &self,
                _topic: Option<$crate::body::TopicKey<'_>>,
                see: &mut dyn FnMut($crate::body::TopicKey<'_>, Option<i32>, i16),
            ) {
                let $topic = self;
                $crate::entry::each_part(&self.$partitions, Some($named), see);
            }
        }
    };
}

/// The entry of one topic that is a part itself, on an API that names no partitions:
/// `topic_part!(Type, |topic| TopicKey::named(&topic.name), error_code_field)`.
macro_rules! topic_part {
    ($entry:ty, |$topic:ident| $named:expr, $error_code:ident) => {
        impl $crate::entry::Entry for $entry {
            fn same(&self, other: &Self) -> bool {
                let one = {
                    let $topic = self;
                    $named
                };
                let other = {
                    let $topic = other;
                    $named
                };
                $crate::body::same_topic(one, other)
            }

            fn absorb(&mut self, other: Self) {
                *self = other;
            }

            fn order_like(&mut self, _pattern: &Self) {}

            fn each_part(
                &self,
                _topic: Option<$crate::body::TopicKey<'_>>,
                see: &mut dyn FnMut($crate::body::TopicKey<'_>, Option<i32>, i16),
            ) {
                let $topic = self;
                see($named, None, self.$error_code);
            }
        }
    };
}

/// An entry that holds topics for one group or one transaction: `group!(Type, key_field, topics_field)`.
macro_rules! group {
    ($entry:ty, $key:ident, $topics:ident) => {
        impl $crate::entry::Entry for $entry {
            fn same(&self, other: &Self) -> bool {
                self.$key == other.$key
            }

            fn absorb(&mut self, other: Self) {
                $crate::entry::merge(&mut self.$topics, other.$topics);
            }

            fn order_like(&mut self, pattern: &Self) {
                $crate::entry::order(&mut self.$topics, &pattern.$topics);
            }

            fn each_part(
                &self,
                topic: Option<$crate::body::TopicKey<'_>>,
                see: &mut dyn FnMut($crate::body::TopicKey<'_>, Option<i32>, i16),
            ) {
                $crate::entry::each_part(&self.$topics, topic, see);
            }
        }
    };
}

/// The response itself, which holds the first entries in one or more lists: `response!(Type, topics_field)`.
macro_rules! response {
    ($response:ty, $($held:ident),+) => {
        impl $crate::entry::Entry for $response {
            fn same(&self, _other: &Self) -> bool {
                true
            }

            fn absorb(&mut self, other: Self) {
                $($crate::entry::merge(&mut self.$held, other.$held);)+
            }

            fn order_like(&mut self, pattern: &Self) {
                $($crate::entry::order(&mut self.$held, &pattern.$held);)+
            }

            fn each_part(
                &self,
                topic: Option<$crate::body::TopicKey<'_>>,
                see: &mut dyn FnMut($crate::body::TopicKey<'_>, Option<i32>, i16),
            ) {
                $($crate::entry::each_part(&self.$held, topic, see);)+
            }
        }
    };
}

/// The methods of [`Editable`](crate::body::Editable) that work on responses, for a request whose response is an [`Entry`].
macro_rules! response_in_parts {
    () => {
        fn merge(truth: &mut Self::Response, failed: Self::Response) {
            $crate::entry::Entry::absorb(truth, failed);
        }

        fn order(response: &mut Self::Response, asked: &Self::Response) {
            $crate::entry::Entry::order_like(response, asked);
        }

        fn each_answered_part(
            response: &Self::Response,
            see: &mut dyn FnMut($crate::body::TopicKey<'_>, Option<i32>, i16),
        ) {
            $crate::entry::Entry::each_part(response, None, see);
        }
    };
}

pub(crate) use {group, partition, response, response_in_parts, topic, topic_part};

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    struct Partition {
        index: i32,
        error_code: i16,
    }

    #[derive(Debug, Clone, PartialEq)]
    struct Topic {
        name: &'static str,
        partitions: Vec<Partition>,
    }

    #[derive(Debug, Clone, PartialEq)]
    struct Response {
        topics: Vec<Topic>,
    }

    partition!(Partition, index, error_code);
    topic!(Topic, |topic| TopicKey::named(topic.name), partitions);
    response!(Response, topics);

    fn topic(name: &'static str, partitions: &[(i32, i16)]) -> Topic {
        Topic {
            name,
            partitions: partitions
                .iter()
                .map(|&(index, error_code)| Partition { index, error_code })
                .collect(),
        }
    }

    #[test]
    fn an_entry_of_both_lists_is_absorbed_and_another_one_is_added() {
        let mut truth = vec![topic("orders", &[(0, 0), (1, 0)])];

        merge(
            &mut truth,
            vec![topic("orders", &[(1, 6), (2, 6)]), topic("payments", &[(0, 6)])],
        );

        assert_eq!(
            truth,
            [topic("orders", &[(0, 0), (1, 6), (2, 6)]), topic("payments", &[(0, 6)])]
        );
    }

    #[test]
    fn entries_take_the_order_of_the_pattern_all_the_way_down() {
        let mut response = vec![topic("payments", &[(0, 0)]), topic("orders", &[(2, 6), (0, 0), (1, 0)])];
        let asked = [topic("orders", &[(0, 0), (1, 0), (2, 0)]), topic("payments", &[(0, 0)])];

        order(&mut response, &asked);

        assert_eq!(
            response,
            [topic("orders", &[(0, 0), (1, 0), (2, 6)]), topic("payments", &[(0, 0)])]
        );
    }

    #[test]
    fn an_entry_the_pattern_does_not_have_comes_last_where_it_stood() {
        let mut response = vec![
            topic("extra.b", &[]),
            topic("orders", &[(7, 0), (0, 0)]),
            topic("extra.a", &[]),
        ];
        let asked = [topic("orders", &[(0, 0)])];

        order(&mut response, &asked);

        assert_eq!(
            response,
            [
                topic("orders", &[(0, 0), (7, 0)]),
                topic("extra.b", &[]),
                topic("extra.a", &[])
            ]
        );
    }

    #[test]
    fn a_response_shows_every_part_with_its_error_code() {
        let response = Response {
            topics: vec![topic("orders", &[(0, 0), (1, 6)]), topic("payments", &[(0, 3)])],
        };

        let mut seen = Vec::new();
        response.each_part(None, &mut |topic, partition, error_code| {
            seen.push((topic.name.to_owned(), partition, error_code));
        });

        assert_eq!(
            seen,
            [
                ("orders".to_owned(), Some(0), 0),
                ("orders".to_owned(), Some(1), 6),
                ("payments".to_owned(), Some(0), 3)
            ]
        );
    }
}
