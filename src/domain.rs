//! Canonical provider-agnostic domain types. Core never branches on
//! [`ProviderKind`]; adapters match it exhaustively at the boundary.

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProviderKind {
    #[serde(rename = "openai")]
    OpenAI,
    #[serde(rename = "openrouter")]
    OpenRouter,
    #[serde(rename = "sail")]
    Sail,
    #[serde(rename = "gemini")]
    Gemini,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BatchStatus {
    Validating,
    InProgress,
    Completed,
    Failed,
    Cancelled,
}

impl BatchStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            BatchStatus::Completed | BatchStatus::Failed | BatchStatus::Cancelled
        )
    }
}

/// Per-item request body dialect. Sniffed per line, never assumed per batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BodyStyle {
    ChatCompletions,
    Responses,
    GeminiNative,
    /// OpenRouter `/v1/embeddings` batches (real endpoint in their enum).
    Embeddings,
}

/// Locked precedence: explicit endpoint match → payload shape → default chat.
/// Non-object or empty-object payloads are unrecognizable (`None`) and become
/// item-level errors at completion, never batch failures.
pub fn sniff_body_style(endpoint: &str, payload: &Value) -> Option<BodyStyle> {
    let ep = endpoint.to_lowercase();
    if ep.contains("embedding") {
        return Some(BodyStyle::Embeddings);
    }
    if ep.contains("responses") {
        return Some(BodyStyle::Responses);
    }
    if ep.contains("chat/completions") || ep.contains("chat_completions") {
        return Some(BodyStyle::ChatCompletions);
    }
    if ep.contains("generatecontent") || ep.contains("generate_content") {
        return Some(BodyStyle::GeminiNative);
    }
    let obj = payload.as_object()?;
    if obj.is_empty() {
        return None;
    }
    if obj.contains_key("contents") {
        return Some(BodyStyle::GeminiNative);
    }
    if obj.contains_key("messages") {
        return Some(BodyStyle::ChatCompletions);
    }
    if obj.contains_key("input") || obj.contains_key("instructions") {
        return Some(BodyStyle::Responses);
    }
    Some(BodyStyle::ChatCompletions)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChaosConfig {
    pub validation_delay_ms: u64,
    pub processing_delay_ms: u64,
    /// Jitter half-width in percent, clamped 0..=100.
    pub jitter_pct: u64,
    /// All rates clamped 0.0..=1.0.
    pub batch_fail_rate: f64,
    pub item_fail_rate: f64,
    pub edge_chaos_rate: f64,
    pub seed: u64,
}

/// Provider-neutral error. Adapters render wire envelopes from this.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimError {
    pub http_status: u16,
    pub code: String,
    pub message: String,
}

impl SimError {
    fn of(http_status: u16, code: &str, message: String) -> Self {
        Self {
            http_status,
            code: code.to_string(),
            message,
        }
    }

    pub fn rate_limit(custom_id: &str) -> Self {
        Self::of(
            429,
            "rate_limit",
            format!("simulated rate limit for item '{custom_id}'"),
        )
    }

    pub fn internal(custom_id: &str) -> Self {
        Self::of(
            500,
            "internal",
            format!("simulated internal error for item '{custom_id}'"),
        )
    }

    pub fn invalid_item(custom_id: &str, detail: &str) -> Self {
        Self::of(
            400,
            "invalid_request",
            format!("item '{custom_id}': {detail}"),
        )
    }

    pub fn batch_failed() -> Self {
        Self::of(
            500,
            "batch_failed",
            "simulated catastrophic batch failure".to_string(),
        )
    }

    pub fn not_found(what: &str) -> Self {
        Self::of(404, "not_found", format!("{what} not found"))
    }

    pub fn conflict(message: &str) -> Self {
        Self::of(409, "conflict", message.to_string())
    }

    pub fn unauthorized() -> Self {
        Self::of(401, "unauthorized", "invalid or missing API key".to_string())
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::of(400, "bad_request", message.into())
    }

    /// DO-infra failure (SQLite, stub RPC): honest 500, never a 400.
    pub fn internal_error(message: impl Into<String>) -> Self {
        Self::of(500, "internal", message.into())
    }
}

/// Truncated reflection of the normalized input, rendered by adapters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EchoData {
    pub input: Value,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UnifiedOutcome {
    Success(EchoData),
    Error(SimError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnifiedItem {
    pub custom_id: String,
    pub endpoint: String,
    /// `None` = unrecognizable body; materializes as an item error.
    pub body_style: Option<BodyStyle>,
    pub input_payload: Value,
    pub outcome: Option<UnifiedOutcome>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WebhookStatus {
    Pending,
    Delivered,
    Failed,
    Skipped,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RequestCounts {
    pub total: u64,
    pub completed: u64,
    pub failed: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnifiedBatch {
    pub batch_id: String,
    pub provider: ProviderKind,
    pub status: BatchStatus,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub completed_at_ms: Option<u64>,
    pub chaos: ChaosConfig,
    pub callback_url: Option<String>,
    /// Simulator-only: real vendors use dashboard-configured secrets.
    pub webhook_secret: Option<String>,
    pub webhook_status: WebhookStatus,
    pub input_file_id: Option<String>,
    pub counts: RequestCounts,
    pub batch_error: Option<SimError>,
    /// Passthrough for render fidelity (e.g. Gemini `models/{model}` path).
    pub model: Option<String>,
    /// Batch-level endpoint default (create body); per-item `endpoint` wins.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Polls served; drives the deterministic edge-chaos stream.
    #[serde(default)]
    pub poll_count: u64,
    /// Inline items for InlineArray providers (OpenRouter/Sail/Gemini-inline)
    /// are persisted in the coordinator's `items` table; this stays empty there.
    /// Carried only through submit paths, never stored on the batch row.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inline_items: Vec<UnifiedItem>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn sniff_precedence_table() {
        // endpoint wins over payload
        assert_eq!(
            sniff_body_style("/v1/responses", &json!({"messages": []})),
            Some(BodyStyle::Responses)
        );
        assert_eq!(
            sniff_body_style("/v1/chat/completions", &json!({"input": []})),
            Some(BodyStyle::ChatCompletions)
        );
        assert_eq!(
            sniff_body_style("models/g:batchGenerateContent", &json!({"messages": []})),
            Some(BodyStyle::GeminiNative)
        );
        // payload shape
        assert_eq!(
            sniff_body_style("/custom", &json!({"contents": []})),
            Some(BodyStyle::GeminiNative)
        );
        assert_eq!(
            sniff_body_style("/custom", &json!({"messages": []})),
            Some(BodyStyle::ChatCompletions)
        );
        assert_eq!(
            sniff_body_style("/custom", &json!({"input": []})),
            Some(BodyStyle::Responses)
        );
        assert_eq!(
            sniff_body_style("/custom", &json!({"instructions": "x"})),
            Some(BodyStyle::Responses)
        );
        assert_eq!(
            sniff_body_style("/v1/embeddings", &json!({"model": "m", "input": "hi"})),
            Some(BodyStyle::Embeddings)
        );
        // default chat
        assert_eq!(
            sniff_body_style("/custom", &json!({"model": "m"})),
            Some(BodyStyle::ChatCompletions)
        );
        // unrecognizable
        assert_eq!(sniff_body_style("/custom", &json!({})), None);
        assert_eq!(sniff_body_style("/custom", &json!([])), None);
        assert_eq!(sniff_body_style("/custom", &json!("s")), None);
    }

    #[test]
    fn terminal_states() {
        assert!(BatchStatus::Completed.is_terminal());
        assert!(BatchStatus::Failed.is_terminal());
        assert!(BatchStatus::Cancelled.is_terminal());
        assert!(!BatchStatus::Validating.is_terminal());
        assert!(!BatchStatus::InProgress.is_terminal());
    }
}
