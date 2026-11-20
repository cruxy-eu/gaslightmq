//! Rolls the pads of a grid row for one request.

use std::time::Duration;

use gaslightmq_proxy_api::{ApiRow, Percent};
use rand::Rng;

/// The pads that fired for a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct Fired {
    pub error: Option<i16>,
    pub delay: Option<Duration>,
    pub blackhole: bool,
}

impl Fired {
    #[cfg(test)]
    pub fn any(&self) -> bool {
        self.error.is_some() || self.delay.is_some() || self.blackhole
    }
}

fn hit(enabled: bool, probability: Percent, rng: &mut impl Rng) -> bool {
    enabled && probability > 0 && (probability >= 100 || rng.random_range(0..100) < probability)
}

/// Roll every enabled pad independently. A blackhole swallows the request,
/// so an error is pointless next to it and is dropped.
pub(crate) fn roll(row: &ApiRow, rng: &mut impl Rng) -> Fired {
    let blackhole = hit(row.blackhole.enabled, row.blackhole.probability, rng);
    let error = hit(row.error.enabled, row.error.probability, rng).then_some(row.error.code);
    let delay = hit(row.delay.enabled, row.delay.probability, rng)
        .then(|| Duration::from_millis(row.delay.ms));
    Fired { error: if blackhole { None } else { error }, delay, blackhole }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::SmallRng;

    fn rng() -> SmallRng {
        SmallRng::seed_from_u64(7)
    }

    #[test]
    fn disabled_row_never_fires() {
        let row = ApiRow::new(0);
        assert!(!roll(&row, &mut rng()).any());
    }

    #[test]
    fn full_probability_always_fires() {
        let mut row = ApiRow::new(0);
        row.error.enabled = true;
        row.error.code = 6;
        row.delay.enabled = true;
        row.delay.ms = 20;
        let fired = roll(&row, &mut rng());
        assert_eq!(fired.error, Some(6));
        assert_eq!(fired.delay, Some(Duration::from_millis(20)));
        assert!(!fired.blackhole);
    }

    #[test]
    fn blackhole_suppresses_error() {
        let mut row = ApiRow::new(0);
        row.error.enabled = true;
        row.blackhole.enabled = true;
        let fired = roll(&row, &mut rng());
        assert!(fired.blackhole);
        assert_eq!(fired.error, None);
    }

    #[test]
    fn half_probability_fires_about_half_the_time() {
        let mut row = ApiRow::new(0);
        row.error.enabled = true;
        row.error.probability = 50;
        let mut rng = rng();
        let hits = (0..1000).filter(|_| roll(&row, &mut rng).error.is_some()).count();
        assert!((400..600).contains(&hits), "{hits}");
    }

    #[test]
    fn zero_probability_never_fires() {
        let mut row = ApiRow::new(0);
        row.blackhole.enabled = true;
        row.blackhole.probability = 0;
        let mut rng = rng();
        assert!((0..100).all(|_| !roll(&row, &mut rng).blackhole));
    }
}
