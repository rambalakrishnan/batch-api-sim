//! Control plane: `SIM_*` env defaults + per-request `X-Sim-*` headers.
//! Invalid control values fall back to defaults (control plane, not provider
//! wire, so never a 400). Header lookup is caller's duty to lowercase.

use crate::config::{
    ENV_API_KEYS, ENV_BATCH_FAIL_RATE, ENV_EDGE_CHAOS_RATE, ENV_ITEM_FAIL_RATE,
    ENV_JITTER_PCT, ENV_PROCESSING_DELAY_MS, ENV_REQUIRE_AUTH,
    ENV_VALIDATION_DELAY_MS, HDR_CALLBACK_URL, HDR_DELAY_MS, HDR_EDGE_CHAOS_RATE,
    HDR_FAIL_RATE, HDR_ITEM_FAIL_RATE, HDR_SEED, HDR_WEBHOOK_SECRET,
};
use crate::domain::ChaosConfig;

#[derive(Debug, Clone)]
pub struct EnvDefaults {
    pub validation_delay_ms: u64,
    pub processing_delay_ms: u64,
    pub jitter_pct: u64,
    pub batch_fail_rate: f64,
    pub item_fail_rate: f64,
    pub edge_chaos_rate: f64,
    pub require_auth: bool,
    pub api_keys: Vec<String>,
}

impl EnvDefaults {
    /// Plan defaults. Used when env is absent/unparseable and in tests.
    pub fn plan_default() -> Self {
        Self {
            validation_delay_ms: 30_000,
            processing_delay_ms: 300_000,
            jitter_pct: 50,
            batch_fail_rate: 0.0,
            item_fail_rate: 0.0,
            edge_chaos_rate: 0.0,
            require_auth: false,
            api_keys: Vec::new(),
        }
    }

    pub fn from_getter(get: &dyn Fn(&str) -> Option<String>) -> Self {
        let mut d = Self::plan_default();
        if let Some(v) = get(ENV_VALIDATION_DELAY_MS).and_then(|s| s.parse().ok()) {
            d.validation_delay_ms = v;
        }
        if let Some(v) = get(ENV_PROCESSING_DELAY_MS).and_then(|s| s.parse().ok()) {
            d.processing_delay_ms = v;
        }
        if let Some(v) = get(ENV_JITTER_PCT).and_then(|s| s.parse().ok()) {
            d.jitter_pct = v;
        }
        if let Some(v) = get(ENV_BATCH_FAIL_RATE).and_then(|s| s.parse().ok()) {
            d.batch_fail_rate = v;
        }
        if let Some(v) = get(ENV_ITEM_FAIL_RATE).and_then(|s| s.parse().ok()) {
            d.item_fail_rate = v;
        }
        if let Some(v) = get(ENV_EDGE_CHAOS_RATE).and_then(|s| s.parse().ok()) {
            d.edge_chaos_rate = v;
        }
        if let Some(v) = get(ENV_REQUIRE_AUTH) {
            d.require_auth = v == "1" || v.eq_ignore_ascii_case("true");
        }
        if let Some(v) = get(ENV_API_KEYS) {
            d.api_keys = v
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect();
        }
        d
    }
}

#[derive(Debug, Clone)]
pub struct CreateControls {
    pub chaos: ChaosConfig,
    pub callback_url: Option<String>,
    pub webhook_secret: Option<String>,
}

fn parse_rate(raw: Option<String>, fallback: f64) -> f64 {
    raw.and_then(|s| s.parse::<f64>().ok())
        .map(|r| r.clamp(0.0, 1.0))
        .unwrap_or(fallback)
}

/// `headers` maps lowercased names to values. `random_seed` is used only when
/// `X-Sim-Seed` is absent/unparseable; caller echoes `chaos.seed` back.
pub fn parse_create_controls(
    headers: &dyn Fn(&str) -> Option<String>,
    env: &EnvDefaults,
    random_seed: u64,
) -> CreateControls {
    // u64s above 2^53 cannot cross JS-JSON boundaries exactly; clamp all
    // control integers into the safe range (documented wire constraint).
    const SAFE_MAX: u64 = (1 << 53) - 1;
    let mut validation_delay_ms = env.validation_delay_ms.min(SAFE_MAX);
    let mut processing_delay_ms = env.processing_delay_ms.min(SAFE_MAX);
    if let Some(ms) = headers(HDR_DELAY_MS).and_then(|s| s.parse::<u64>().ok()) {
        validation_delay_ms = ms.min(SAFE_MAX);
        processing_delay_ms = ms.min(SAFE_MAX);
    }
    let seed = headers(HDR_SEED)
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(random_seed)
        & SAFE_MAX;
    CreateControls {
        chaos: ChaosConfig {
            validation_delay_ms,
            processing_delay_ms,
            jitter_pct: env.jitter_pct.min(100),
            batch_fail_rate: parse_rate(headers(HDR_FAIL_RATE), env.batch_fail_rate),
            item_fail_rate: parse_rate(headers(HDR_ITEM_FAIL_RATE), env.item_fail_rate),
            edge_chaos_rate: parse_rate(headers(HDR_EDGE_CHAOS_RATE), env.edge_chaos_rate),
            seed,
        },
        callback_url: headers(HDR_CALLBACK_URL).filter(|s| !s.is_empty()),
        webhook_secret: headers(HDR_WEBHOOK_SECRET).filter(|s| !s.is_empty()),
    }
}

/// Uniform auth gate. `None` = pass; `Some(SimError::unauthorized)` = reject.
pub fn check_auth(
    authorization: Option<&str>,
    env: &EnvDefaults,
) -> Option<crate::domain::SimError> {
    if !env.require_auth {
        return None;
    }
    let token = authorization
        .and_then(|h| h.strip_prefix("Bearer "))
        .map(str::trim)
        .unwrap_or("");
    if !token.is_empty() && env.api_keys.iter().any(|k| k == token) {
        None
    } else {
        Some(crate::domain::SimError::unauthorized())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn hmap(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn headers_override_env_and_seed_echoes() {
        let env = EnvDefaults::plan_default();
        let h = hmap(&[
            (HDR_DELAY_MS, "50"),
            (HDR_FAIL_RATE, "0.5"),
            (HDR_SEED, "1234"),
            (HDR_CALLBACK_URL, "https://cb.test/hook"),
        ]);
        let c = parse_create_controls(&|k: &str| h.get(k).cloned(), &env, 999);
        assert_eq!(c.chaos.validation_delay_ms, 50);
        assert_eq!(c.chaos.processing_delay_ms, 50);
        assert_eq!(c.chaos.batch_fail_rate, 0.5);
        assert_eq!(c.chaos.seed, 1234);
        assert_eq!(c.callback_url.as_deref(), Some("https://cb.test/hook"));
        assert_eq!(c.chaos.item_fail_rate, 0.0);
    }

    #[test]
    fn invalid_values_fall_back_silently() {
        let env = EnvDefaults::plan_default();
        let h = hmap(&[
            (HDR_DELAY_MS, "soon"),
            (HDR_FAIL_RATE, "all"),
            (HDR_SEED, "abc"),
        ]);
        let c = parse_create_controls(&|k: &str| h.get(k).cloned(), &env, 999);
        assert_eq!(c.chaos.validation_delay_ms, 30_000);
        assert_eq!(c.chaos.batch_fail_rate, 0.0);
        assert_eq!(c.chaos.seed, 999);
        // rates clamp
        let h = hmap(&[(HDR_FAIL_RATE, "9.0"), (HDR_ITEM_FAIL_RATE, "-2.0")]);
        let c = parse_create_controls(&|k: &str| h.get(k).cloned(), &env, 1);
        assert_eq!(c.chaos.batch_fail_rate, 1.0);
        assert_eq!(c.chaos.item_fail_rate, 0.0);
    }

    #[test]
    fn unsafe_integers_clamp_to_2pow53() {
        let env = EnvDefaults::plan_default();
        let h = hmap(&[(HDR_DELAY_MS, "18446744073709551615"), (HDR_SEED, "9991488526391367000")]);
        let c = parse_create_controls(&|k: &str| h.get(k).cloned(), &env, u64::MAX);
        assert_eq!(c.chaos.validation_delay_ms, (1 << 53) - 1);
        assert!(c.chaos.seed < (1 << 53));
        // small values untouched
        let h = hmap(&[(HDR_DELAY_MS, "50"), (HDR_SEED, "7")]);
        let c = parse_create_controls(&|k: &str| h.get(k).cloned(), &env, 1);
        assert_eq!(c.chaos.validation_delay_ms, 50);
        assert_eq!(c.chaos.seed, 7);
    }

    #[test]
    fn auth_gate() {
        let open = EnvDefaults::plan_default();
        assert!(check_auth(None, &open).is_none());
        assert!(check_auth(Some("Bearer anything"), &open).is_none());
        let mut strict = EnvDefaults::plan_default();
        strict.require_auth = true;
        strict.api_keys = vec!["k1".to_string()];
        assert!(check_auth(None, &strict).is_some());
        assert!(check_auth(Some("Bearer nope"), &strict).is_some());
        assert!(check_auth(Some("Bearer k1"), &strict).is_none());
    }
}
