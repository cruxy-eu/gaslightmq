//! The presences of the clients of the live stream: which of them the control plane holds, when one ends,
//! and whether the client of a stream still answers.
//!
//! The module decides and never waits: whoever serves a stream tells it what happened and when,
//! and waits itself for the instant that it names.

use std::collections::BTreeMap;

use gaslightmq_control_api::clock::{Instant, TimeSpan};
use gaslightmq_control_api::surface::PresenceId;

/// The key of the query of the live stream that names the presence a client comes back as.
const PRESENCE_KEY: &str = "presence";

/// The presence that a client comes back as: the one that the query of its live stream names.
///
/// `None` for a query that names none, and for one that names what is no presence ID:
/// the control plane holds no such presence, so the client is new.
pub(crate) fn asked_for(query: &str) -> Option<PresenceId> {
    query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .find(|(key, _)| *key == PRESENCE_KEY)
        .and_then(|(_, presence)| presence.parse().ok())
}

/// The presences that the control plane holds: the clients of the live stream that are there,
/// and those whose stream ended a moment ago.
///
/// A presence is there while a stream of it is open.
/// When its last stream ends, the control plane keeps it for the grace period:
/// a client that [comes back](Self::resumed) within it goes on as the same presence.
/// A client that stays away is gone, and its presence [has ended](Self::ended).
///
/// A client that comes back can do so before the control plane noticed that its earlier stream is gone,
/// so a presence can have several streams at once, and it is there until the last of them ends.
#[derive(Debug)]
pub(crate) struct Presences {
    /// How long a presence stays once its last stream ended.
    grace: TimeSpan,
    held: BTreeMap<PresenceId, Standing>,
    /// Whether the control plane stops, so that no presence opens any more.
    stopped: bool,
}

/// Where one presence stands.
#[derive(Debug)]
struct Standing {
    /// How many streams of the presence are open.
    streams: usize,
    /// When the presence ends unless its client comes back. `None` while a stream of it is open.
    ends_at: Option<Instant>,
}

impl Presences {
    /// No presence yet. A presence stays for `grace` once its last stream ended.
    pub(crate) const fn new(grace: TimeSpan) -> Self {
        Self {
            grace,
            held: BTreeMap::new(),
            stopped: false,
        }
    }

    /// A stream opened for a client that is new, as `presence`.
    ///
    /// Returns whether the control plane holds the presence now. It holds none once it [stops](Self::end_all).
    pub(crate) fn opened(&mut self, presence: PresenceId) -> bool {
        if self.stopped {
            return false;
        }
        let standing = self.held.entry(presence).or_insert(Standing {
            streams: 0,
            ends_at: None,
        });
        standing.streams += 1;
        standing.ends_at = None;
        true
    }

    /// A stream opened at `now` for a client that comes back as `presence`.
    ///
    /// Returns whether the control plane still holds the presence, so the client goes on as it.
    /// It does not when the client stayed away for the grace period, or never was this presence here.
    /// A presence whose grace period is over has ended, also when nobody [asked](Self::ended) yet.
    pub(crate) fn resumed(&mut self, presence: PresenceId, now: Instant) -> bool {
        let Some(standing) = self.held.get_mut(&presence) else {
            return false;
        };
        if standing.ends_at.is_some_and(|ends_at| now >= ends_at) {
            return false;
        }
        standing.streams += 1;
        standing.ends_at = None;
        true
    }

    /// A stream of `presence` ended at `now`.
    ///
    /// Returns the instant at which the presence [ends](Self::ended) unless its client comes back.
    /// `None` while another stream of the presence is open, and for a presence that the control plane does not hold.
    pub(crate) fn left(&mut self, presence: PresenceId, now: Instant) -> Option<Instant> {
        let standing = self.held.get_mut(&presence)?;
        standing.streams = standing.streams.saturating_sub(1);
        if standing.streams > 0 {
            return None;
        }
        let ends_at = now + self.grace;
        standing.ends_at = Some(ends_at);
        Some(ends_at)
    }

    /// Whether `presence` has ended by `now`: its client stayed away for the grace period.
    ///
    /// The control plane holds the presence no more then, and the caller ends what belonged to it.
    /// A presence whose client came back has not ended, also when it left again since: it ends a grace period after
    /// it left last.
    pub(crate) fn ended(&mut self, presence: PresenceId, now: Instant) -> bool {
        let gone = self
            .held
            .get(&presence)
            .and_then(|standing| standing.ends_at)
            .is_some_and(|ends_at| now >= ends_at);
        if gone {
            self.held.remove(&presence);
        }
        gone
    }

    /// The control plane stops: no client comes back to it, so every presence ends at once, and none opens any more.
    ///
    /// Returns the presences that it held, for the caller to end what belonged to them.
    pub(crate) fn end_all(&mut self) -> Vec<PresenceId> {
        self.stopped = true;
        std::mem::take(&mut self.held).into_keys().collect()
    }
}

/// Whether the client of one live stream still answers.
///
/// A connection can go silent without closing: the network of its client dropped, or its laptop sleeps.
/// So the server pings the client at an interval, and a client that is there answers every ping.
/// A client that sent nothing between one ping and the next has gone silent,
/// and so has one that takes no message for as long as an interval.
#[derive(Debug)]
pub(crate) struct Liveness {
    /// How long the server waits between two pings.
    interval: TimeSpan,
    /// When the next ping is due.
    next_ping: Instant,
    /// When the server pinged the client last. `None` before the first ping.
    pinged_at: Option<Instant>,
    /// When the client sent something last, or when its stream opened.
    heard_at: Instant,
}

/// What the server does when a ping is due.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PingDue {
    /// The client answered the ping before, or this is the first one: it gets a ping.
    Ping,
    /// The client sent nothing since the ping before. It has been silent for a whole interval at `since`,
    /// the instant at which this ping was due.
    Silent { since: Instant },
}

impl Liveness {
    /// The liveness of a client whose stream opened at `now`, which the server pings every `interval`.
    pub(crate) fn new(interval: TimeSpan, now: Instant) -> Self {
        Self {
            interval,
            next_ping: now + interval,
            pinged_at: None,
            heard_at: now,
        }
    }

    /// When the next ping is due: an interval after the one before, or after the stream opened.
    pub(crate) const fn next_ping(&self) -> Instant {
        self.next_ping
    }

    /// The client sent something at `now`: the answer to a ping, or anything else.
    pub(crate) const fn heard(&mut self, now: Instant) {
        self.heard_at = now;
    }

    /// A ping is due, and the server gets to it at `now`.
    ///
    /// A client that sent something since the ping before is there, and gets the next ping.
    /// A client that sent nothing has gone silent. It is silent since the instant at which this ping was due,
    /// also when the server gets to it later.
    pub(crate) fn due(&mut self, now: Instant) -> PingDue {
        if self.pinged_at.is_some_and(|pinged_at| self.heard_at < pinged_at) {
            return PingDue::Silent { since: self.next_ping };
        }
        self.pinged_at = Some(now);
        self.next_ping = now + self.interval;
        PingDue::Ping
    }

    /// The instant by which a client that is there has taken a message that the server sends at `now`.
    /// A client that has not taken it by then reads nothing any more, and is silent since that instant.
    pub(crate) fn taken_by(&self, now: Instant) -> Instant {
        now + self.interval
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The presences of two tabs of the UI.
    const ANAS_TAB: PresenceId = PresenceId::from_random_bits(0xa);
    const DANS_TAB: PresenceId = PresenceId::from_random_bits(0xd);

    /// The grace period of the tests.
    const GRACE: TimeSpan = TimeSpan::from_secs(10);

    /// The moment at which the stream of a test ends.
    const LEFT_AT: Instant = Instant::from_nanos(7_000_000_000);

    /// Half-way through the grace period of a stream that ended at [`LEFT_AT`].
    fn within_the_grace() -> Instant {
        LEFT_AT + GRACE.duration() / 2
    }

    #[test]
    fn a_presence_whose_client_stays_away_ends_after_the_grace_period() {
        let mut presences = Presences::new(GRACE);
        assert!(presences.opened(ANAS_TAB));

        let ends_at = presences.left(ANAS_TAB, LEFT_AT);

        assert_eq!(ends_at, Some(LEFT_AT + GRACE));
        assert!(!presences.ended(ANAS_TAB, within_the_grace()));
        assert!(presences.ended(ANAS_TAB, LEFT_AT + GRACE));
        // It ended once, and a client that comes back now is new.
        assert!(!presences.ended(ANAS_TAB, LEFT_AT + GRACE));
        assert!(!presences.resumed(ANAS_TAB, LEFT_AT + GRACE));
    }

    #[test]
    fn a_client_that_comes_back_after_the_grace_period_is_new_also_before_anyone_asked_whether_its_presence_ended() {
        let mut presences = Presences::new(GRACE);
        presences.opened(ANAS_TAB);
        presences.left(ANAS_TAB, LEFT_AT);

        assert!(!presences.resumed(ANAS_TAB, LEFT_AT + GRACE));

        // The presence has ended, and whoever waited for it learns so.
        assert!(presences.ended(ANAS_TAB, LEFT_AT + GRACE));
    }

    #[test]
    fn a_client_that_comes_back_within_the_grace_period_goes_on_as_its_presence() {
        let mut presences = Presences::new(GRACE);
        presences.opened(ANAS_TAB);
        presences.left(ANAS_TAB, LEFT_AT);

        assert!(presences.resumed(ANAS_TAB, within_the_grace()));

        // The grace period of the stream that ended is over, and the presence is there.
        assert!(!presences.ended(ANAS_TAB, LEFT_AT + GRACE));
        // When it leaves again, it ends a grace period after that.
        let left_again = LEFT_AT + GRACE;
        assert_eq!(presences.left(ANAS_TAB, left_again), Some(left_again + GRACE));
        assert!(presences.ended(ANAS_TAB, left_again + GRACE));
    }

    #[test]
    fn a_presence_is_there_until_its_last_stream_ends() {
        let mut presences = Presences::new(GRACE);
        presences.opened(ANAS_TAB);
        // The client comes back before the control plane noticed that its first stream is gone.
        assert!(presences.resumed(ANAS_TAB, LEFT_AT));

        // The first stream ends at last, and the presence is still there.
        assert_eq!(presences.left(ANAS_TAB, LEFT_AT), None);
        assert!(!presences.ended(ANAS_TAB, LEFT_AT + GRACE));
        assert_eq!(presences.left(ANAS_TAB, LEFT_AT), Some(LEFT_AT + GRACE));
    }

    #[test]
    fn a_presence_ends_without_the_others() {
        let mut presences = Presences::new(GRACE);
        presences.opened(ANAS_TAB);
        presences.opened(DANS_TAB);
        presences.left(ANAS_TAB, LEFT_AT);

        assert!(presences.ended(ANAS_TAB, LEFT_AT + GRACE));
        assert!(!presences.ended(DANS_TAB, LEFT_AT + GRACE));
        assert!(presences.resumed(DANS_TAB, LEFT_AT + GRACE));
    }

    #[test]
    fn a_grace_period_of_no_time_ends_a_presence_with_its_stream() {
        let mut presences = Presences::new(TimeSpan::ZERO);
        presences.opened(ANAS_TAB);

        assert_eq!(presences.left(ANAS_TAB, LEFT_AT), Some(LEFT_AT));
        assert!(presences.ended(ANAS_TAB, LEFT_AT));
    }

    #[test]
    fn a_control_plane_that_stops_ends_every_presence_and_opens_none() {
        let mut presences = Presences::new(GRACE);
        presences.opened(ANAS_TAB);
        presences.opened(DANS_TAB);
        presences.left(DANS_TAB, LEFT_AT);

        assert_eq!(presences.end_all(), [ANAS_TAB, DANS_TAB]);

        assert!(!presences.opened(ANAS_TAB));
        assert!(!presences.resumed(DANS_TAB, LEFT_AT));
        // The stream of a presence that ended with the control plane leaves nothing to wait for.
        assert_eq!(presences.left(ANAS_TAB, LEFT_AT), None);
    }

    #[test]
    fn the_query_of_a_stream_names_the_presence_that_its_client_comes_back_as() {
        let presence: PresenceId = "9f86d081884c7d659a2feaa0c55ad015".parse().expect("a presence ID");

        assert_eq!(asked_for("presence=9f86d081884c7d659a2feaa0c55ad015"), Some(presence));
        assert_eq!(
            asked_for("token=s3cr3t&presence=9f86d081884c7d659a2feaa0c55ad015"),
            Some(presence)
        );
        // A client that names no presence, or what is none, is new.
        assert_eq!(asked_for(""), None);
        assert_eq!(asked_for("presence"), None);
        assert_eq!(asked_for("presence=tab-7"), None);
    }

    /// How often the server of the tests pings a client.
    const INTERVAL: TimeSpan = TimeSpan::from_secs(15);

    /// The moment at which the stream of a liveness test opens.
    const OPENED_AT: Instant = Instant::from_nanos(2_000_000_000);

    #[test]
    fn a_client_that_answers_every_ping_gets_the_next_one() {
        let mut liveness = Liveness::new(INTERVAL, OPENED_AT);
        let first = OPENED_AT + INTERVAL;
        assert_eq!(liveness.next_ping(), first);

        // The first ping has no ping before it that the client could have left unanswered.
        assert_eq!(liveness.due(first), PingDue::Ping);
        assert_eq!(liveness.next_ping(), first + INTERVAL);
        // The client answers at once, in the same instant as the ping on a clock that someone steps.
        liveness.heard(first);

        assert_eq!(liveness.due(first + INTERVAL), PingDue::Ping);
    }

    #[test]
    fn a_client_that_sends_nothing_between_two_pings_has_gone_silent_when_the_second_was_due() {
        let mut liveness = Liveness::new(INTERVAL, OPENED_AT);
        let first = OPENED_AT + INTERVAL;
        // The client said something before the first ping, and nothing after it.
        liveness.heard(OPENED_AT);
        assert_eq!(liveness.due(first), PingDue::Ping);

        // The server gets to the second ping late, and the client is silent since the ping was due.
        let late = first + INTERVAL + INTERVAL;

        assert_eq!(
            liveness.due(late),
            PingDue::Silent {
                since: first + INTERVAL
            }
        );
    }

    #[test]
    fn anything_that_a_client_sends_says_that_it_is_there() {
        let mut liveness = Liveness::new(INTERVAL, OPENED_AT);
        let first = OPENED_AT + INTERVAL;
        assert_eq!(liveness.due(first), PingDue::Ping);

        // A subscription comes half-way to the next ping, and no answer to the ping itself.
        liveness.heard(first + INTERVAL.duration() / 2);

        assert_eq!(liveness.due(first + INTERVAL), PingDue::Ping);
    }

    #[test]
    fn a_client_takes_a_message_within_one_interval() {
        let liveness = Liveness::new(INTERVAL, OPENED_AT);

        assert_eq!(liveness.taken_by(OPENED_AT), OPENED_AT + INTERVAL);
    }
}
