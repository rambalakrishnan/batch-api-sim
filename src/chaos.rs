//! Seeded per-batch randomness. Same seed + same call sequence ⇒
//! byte-identical outcomes. Polls use a separate stream so edge chaos never
//! consumes outcome RNG.

use fastrand::Rng;

use crate::config::EDGE_429_SHARE;

pub struct ChaosEngine {
    rng: Rng,
    edge_rng: Rng,
    jitter_pct: u64,
}

impl ChaosEngine {
    pub fn new(seed: u64, jitter_pct: u64) -> Self {
        Self {
            rng: Rng::with_seed(seed),
            // Split-stream constant; any fixed odd value works.
            edge_rng: Rng::with_seed(seed ^ 0x9e3779b97f4a7c15),
            jitter_pct: jitter_pct.min(100),
        }
    }

    /// `base ± jitter_pct%`, rounded. `jitter(0) == 0` (fast-forward path).
    pub fn jitter(&mut self, base_ms: u64) -> u64 {
        if base_ms == 0 || self.jitter_pct == 0 {
            return base_ms;
        }
        let frac = self.rng.f64() * 2.0 - 1.0;
        let delta = base_ms as f64 * (self.jitter_pct as f64 / 100.0) * frac;
        base_ms.saturating_add_signed(delta.round() as i64)
    }

    pub fn roll_bool(&mut self, rate: f64) -> bool {
        let rate = rate.clamp(0.0, 1.0);
        if rate <= 0.0 {
            return false;
        }
        if rate >= 1.0 {
            return true;
        }
        self.rng.f64() < rate
    }

    /// Seeded 50/50 transient-vs-hard item error.
    pub fn pick_item_error(&mut self) -> u16 {
        if self.rng.f64() < 0.5 { 429 } else { 500 }
    }

    /// Edge-chaos roll on its own stream: `None` or a transient status.
    pub fn edge_chaos(&mut self, rate: f64) -> Option<u16> {
        let rate = rate.clamp(0.0, 1.0);
        if rate <= 0.0 || self.edge_rng.f64() >= rate {
            return None;
        }
        if self.edge_rng.f64() < EDGE_429_SHARE {
            Some(429)
        } else {
            Some(503)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_sequence() {
        let run = || {
            let mut e = ChaosEngine::new(42, 50);
            let j = (e.jitter(1000), e.jitter(5000));
            let r = (0..8).map(|_| e.roll_bool(0.5)).collect::<Vec<_>>();
            let errs = (0..8).map(|_| e.pick_item_error()).collect::<Vec<_>>();
            let edge = (0..8).map(|_| e.edge_chaos(0.5)).collect::<Vec<_>>();
            (j, r, errs, edge)
        };
        assert_eq!(run(), run());
    }

    #[test]
    fn edge_stream_does_not_consume_outcome_rng() {
        let mut a = ChaosEngine::new(7, 50);
        let mut b = ChaosEngine::new(7, 50);
        for _ in 0..100 {
            a.edge_chaos(1.0);
        }
        assert_eq!(a.jitter(1000), b.jitter(1000));
        assert_eq!(a.roll_bool(0.3), b.roll_bool(0.3));
    }

    #[test]
    fn rate_edges_and_zero_jitter() {
        let mut e = ChaosEngine::new(1, 50);
        assert!(!e.roll_bool(0.0));
        assert!(e.roll_bool(1.0));
        assert_eq!(e.jitter(0), 0);
        assert_eq!(ChaosEngine::new(1, 0).jitter(1000), 1000);
    }

    #[test]
    fn jitter_stays_in_band() {
        let mut e = ChaosEngine::new(99, 50);
        for _ in 0..1000 {
            let j = e.jitter(1000);
            assert!((500..=1500).contains(&j), "{j}");
        }
    }
}
