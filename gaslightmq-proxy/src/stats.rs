//! Lock-free activity counters, turned into periodic deltas for the UI.

use std::sync::atomic::{AtomicU64, Ordering};

use gaslightmq_proxy_api::ApiTick;

const API_SLOTS: usize = 256;

#[derive(Default)]
struct ApiCounters {
    requests: AtomicU64,
    errors: AtomicU64,
    delays: AtomicU64,
    blackholes: AtomicU64,
}

impl ApiCounters {
    fn load(&self, api_key: i16) -> ApiTick {
        ApiTick {
            api_key,
            requests: self.requests.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            delays: self.delays.load(Ordering::Relaxed),
            blackholes: self.blackholes.load(Ordering::Relaxed),
        }
    }
}

pub(crate) struct Stats {
    apis: Box<[ApiCounters]>,
}

impl Default for Stats {
    fn default() -> Self {
        Self { apis: (0..API_SLOTS).map(|_| ApiCounters::default()).collect() }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Event {
    Request,
    Error,
    Delay,
    Blackhole,
}

impl Stats {
    pub fn record(&self, api_key: i16, event: Event) {
        let Some(c) = usize::try_from(api_key).ok().and_then(|i| self.apis.get(i)) else {
            return;
        };
        let counter = match event {
            Event::Request => &c.requests,
            Event::Error => &c.errors,
            Event::Delay => &c.delays,
            Event::Blackhole => &c.blackholes,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Totals for every API that saw traffic.
    pub fn totals(&self) -> Vec<ApiTick> {
        self.apis
            .iter()
            .enumerate()
            .map(|(i, c)| c.load(i as i16))
            .filter(|t| t.requests > 0)
            .collect()
    }
}

/// Difference between two sets of totals, dropping APIs without activity.
pub(crate) fn deltas(now: &[ApiTick], before: &[ApiTick]) -> Vec<ApiTick> {
    now.iter()
        .filter_map(|n| {
            let b = before.iter().find(|b| b.api_key == n.api_key).copied().unwrap_or_default();
            let d = ApiTick {
                api_key: n.api_key,
                requests: n.requests - b.requests,
                errors: n.errors - b.errors,
                delays: n.delays - b.delays,
                blackholes: n.blackholes - b.blackholes,
            };
            (d.requests + d.errors + d.delays + d.blackholes > 0).then_some(d)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deltas_only_list_active_apis() {
        let stats = Stats::default();
        stats.record(0, Event::Request);
        stats.record(3, Event::Request);
        let first = stats.totals();
        stats.record(0, Event::Request);
        stats.record(0, Event::Error);
        stats.record(-1, Event::Request);
        let d = deltas(&stats.totals(), &first);
        assert_eq!(d, [ApiTick { api_key: 0, requests: 1, errors: 1, delays: 0, blackholes: 0 }]);
    }
}
