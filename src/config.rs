//! Locked constants from the grilled plan. Change only via plan amendment.

// ---------- quotas (plan Step 1) ----------
pub const MAX_ITEMS_PER_BATCH: usize = 10_000;
pub const MAX_UPLOAD_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_LINE_BYTES: usize = 1024 * 1024;

// ---------- registry ----------
pub const REGISTRY_DO_NAME: &str = "global-file-registry";
pub const UNREFERENCED_TTL_MS: i64 = 86_400_000; // 24h
pub const REFERENCED_TTL_MS: i64 = 604_800_000; // 7d
pub const REGISTRY_CLEANUP_INTERVAL_MS: i64 = 3_600_000; // hourly prune sweep
pub const COPY_PAGE_ROWS: i64 = 256;

// ---------- rendering ----------
pub const ECHO_CAP_BYTES: usize = 8 * 1024;
pub const GEMINI_INLINE_LIMIT_BYTES: usize = 262_144; // 256 KiB

// ---------- webhooks ----------
pub const WEBHOOK_MAX_ATTEMPTS: u32 = 3;
pub const WEBHOOK_BACKOFF_MS: [u64; 2] = [500, 2000];
/// Webhook payloads embed at most this many items; receivers GET the rest.
pub const WEBHOOK_MAX_ITEMS: usize = 100;

// ---------- edge chaos ----------
pub const RETRY_AFTER_SECS: u64 = 1;
/// P(429 | edge-chaos hit); remainder is 503.
pub const EDGE_429_SHARE: f64 = 0.7;

// ---------- control headers (exact literals, matched case-insensitively) ----------
pub const HDR_DELAY_MS: &str = "x-sim-delay-ms";
pub const HDR_FAIL_RATE: &str = "x-sim-fail-rate";
pub const HDR_ITEM_FAIL_RATE: &str = "x-sim-item-fail-rate";
pub const HDR_EDGE_CHAOS_RATE: &str = "x-sim-edge-chaos-rate";
pub const HDR_SEED: &str = "x-sim-seed";
pub const HDR_SEED_USED: &str = "x-sim-seed-used";
pub const HDR_CALLBACK_URL: &str = "x-sim-callback-url";
pub const HDR_WEBHOOK_SECRET: &str = "x-sim-webhook-secret";
pub const HDR_DEDUPE_KEY: &str = "dedupe-key";
pub const HDR_WEBHOOK_SIG: &str = "x-webhook-signature";

// ---------- env vars (exact literals) ----------
pub const ENV_VALIDATION_DELAY_MS: &str = "SIM_VALIDATION_DELAY_MS";
pub const ENV_PROCESSING_DELAY_MS: &str = "SIM_PROCESSING_DELAY_MS";
pub const ENV_JITTER_PCT: &str = "SIM_JITTER_PCT";
pub const ENV_BATCH_FAIL_RATE: &str = "SIM_BATCH_FAIL_RATE";
pub const ENV_ITEM_FAIL_RATE: &str = "SIM_ITEM_FAIL_RATE";
pub const ENV_EDGE_CHAOS_RATE: &str = "SIM_EDGE_CHAOS_RATE";
pub const ENV_REQUIRE_AUTH: &str = "SIM_REQUIRE_AUTH";
pub const ENV_API_KEYS: &str = "SIM_API_KEYS";

// ---------- id formats ----------
pub const OUTPUT_FILE_PREFIX: &str = "file-out-";
pub const ERROR_FILE_PREFIX: &str = "file-err-";

pub fn output_file_id(batch_id: &str) -> String {
    format!("{OUTPUT_FILE_PREFIX}{batch_id}")
}

pub fn error_file_id(batch_id: &str) -> String {
    format!("{ERROR_FILE_PREFIX}{batch_id}")
}

/// If `file_id` is an embedded-output id, returns `(batch_id, is_error_file)`.
pub fn parse_embedded_file_id(file_id: &str) -> Option<(&str, bool)> {
    if let Some(rest) = file_id.strip_prefix(OUTPUT_FILE_PREFIX) {
        return Some((rest, false));
    }
    if let Some(rest) = file_id.strip_prefix(ERROR_FILE_PREFIX) {
        return Some((rest, true));
    }
    None
}

/// Random `batch_<32 hex>` id. Provider prefixes are a render concern.
pub fn new_batch_id() -> String {
    format!("batch_{:016x}{:016x}", fastrand::u64(..), fastrand::u64(..))
}

/// Random OpenAI-shaped `file_<24 hex>` id.
pub fn new_file_id() -> String {
    format!(
        "file_{:016x}{:08x}",
        fastrand::u64(..),
        fastrand::u32(..)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_ids_round_trip() {
        let out = output_file_id("batch_abc");
        let err = error_file_id("batch_abc");
        assert_eq!(parse_embedded_file_id(&out), Some(("batch_abc", false)));
        assert_eq!(parse_embedded_file_id(&err), Some(("batch_abc", true)));
        assert_eq!(parse_embedded_file_id("file_abc"), None);
        assert_eq!(parse_embedded_file_id("batch_abc"), None);
    }
}
