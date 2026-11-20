//! Shared adapter helpers: ingest/render protocol types, success
//! skeletons per [`BodyStyle`], echo truncation, timestamps. Adapters supply
//! only wire-shape closures over these.

use serde_json::{Map, Value};

use crate::config::ECHO_CAP_BYTES;
use crate::domain::{BodyStyle, EchoData, SimError, UnifiedItem};

// ---------------------------------------------------------------------------
// protocol types
// ---------------------------------------------------------------------------

/// Where a batch's items come from.
#[derive(Debug)]
pub enum ItemSource {
    Inline(Vec<UnifiedItem>),
    FileRef(String),
}

/// Where a batch's results go.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseSink {
    InlineArray,
    JsonlStream,
}

#[derive(Debug)]
pub struct CreateIntent {
    pub source: ItemSource,
    pub callback_url: Option<String>,
    pub model: Option<String>,
    pub endpoint: Option<String>,
}

#[derive(Debug)]
pub enum IngestRequest<'a> {
    CreateBody(&'a Value),
    InlineUnit { unit: &'a Value, endpoint: &'a str },
    FileLine { line: &'a str, number: usize },
}

#[derive(Debug)]
pub enum IngestOutcome {
    CreateIntent(CreateIntent),
    Item(UnifiedItem),
}

/// Primary results document. File-mode providers with split outputs
/// (OpenAI) return the error document separately.
#[derive(Debug)]
pub enum RenderedResults {
    Json(Value),
    Jsonl(String),
}

/// `primary` feeds `output_file_id`/inline results; `secondary` (when
/// present) feeds `error_file_id`.
#[derive(Debug)]
pub struct RenderedFiles {
    pub primary: RenderedResults,
    pub secondary: Option<RenderedResults>,
}

impl RenderedFiles {
    pub fn json(primary: Value) -> Self {
        Self {
            primary: RenderedResults::Json(primary),
            secondary: None,
        }
    }

    pub fn jsonl(primary: String, secondary: Option<String>) -> Self {
        Self {
            primary: RenderedResults::Jsonl(primary),
            secondary: secondary.map(RenderedResults::Jsonl),
        }
    }
}

// ---------------------------------------------------------------------------
// field extraction (400s, never panics)
// ---------------------------------------------------------------------------

pub fn req_str(obj: &Map<String, Value>, field: &str, ctx: &str) -> Result<String, SimError> {
    match obj.get(field) {
        Some(Value::String(s)) if !s.is_empty() => Ok(s.clone()),
        _ => Err(SimError::bad_request(format!(
            "{ctx}: '{field}' is required and must be a non-empty string"
        ))),
    }
}

pub fn opt_str(obj: &Map<String, Value>, field: &str) -> Option<String> {
    obj.get(field).and_then(|v| v.as_str()).map(str::to_string)
}

pub fn req_obj<'a>(obj: &'a Map<String, Value>, field: &str, ctx: &str) -> Result<&'a Value, SimError> {
    match obj.get(field) {
        Some(v @ Value::Object(_)) => Ok(v),
        _ => Err(SimError::bad_request(format!(
            "{ctx}: '{field}' is required and must be an object"
        ))),
    }
}

pub fn as_obj<'a>(v: &'a Value, ctx: &str) -> Result<&'a Map<String, Value>, SimError> {
    v.as_object()
        .ok_or_else(|| SimError::bad_request(format!("{ctx}: must be a JSON object")))
}

// ---------------------------------------------------------------------------
// echo truncation (deep echo with bounded rows)
// ---------------------------------------------------------------------------

pub fn truncate_echo(payload: &Value) -> EchoData {
    let serialized = serde_json::to_string(payload).unwrap_or_default();
    if serialized.len() <= ECHO_CAP_BYTES {
        return EchoData {
            input: payload.clone(),
            truncated: false,
        };
    }
    let mut preview = serialized.clone();
    preview.truncate(1024);
    EchoData {
        input: serde_json::json!({
            "sim_truncated": true,
            "original_bytes": serialized.len(),
            "preview": preview,
        }),
        truncated: true,
    }
}

// ---------------------------------------------------------------------------
// timestamps
// ---------------------------------------------------------------------------

pub fn unix_secs(ms: u64) -> u64 {
    ms / 1000
}

/// `ms` since epoch → `YYYY-MM-DDTHH:MM:SS.mmmZ`. No chrono (wasm budget).
pub fn ms_to_rfc3339(ms: u64) -> String {
    let secs = ms / 1000;
    let millis = ms % 1000;
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{millis:03}Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

/// Howard Hinnant's days→civil algorithm. `days` may be negative pre-1970.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

// ---------------------------------------------------------------------------
// success skeletons (schema-valid, zero usage, deep echo attached)
// ---------------------------------------------------------------------------

pub const SIM_MODEL: &str = "sim-mock-1";

fn echo_json(echo: &EchoData) -> Value {
    serde_json::json!({ "input": echo.input, "truncated": echo.truncated })
}

pub fn chat_skeleton(custom_id: &str, echo: &EchoData, created_ms: u64) -> Value {
    serde_json::json!({
        "id": format!("chatcmpl-sim-{custom_id}"),
        "object": "chat.completion",
        "created": unix_secs(created_ms),
        "model": SIM_MODEL,
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": format!("sim-echo:{custom_id}"),
                "refusal": null,
            },
            "logprobs": null,
            "finish_reason": "stop",
        }],
        "usage": {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        },
        "sim_echo": echo_json(echo),
    })
}

pub fn responses_skeleton(custom_id: &str, echo: &EchoData, created_ms: u64) -> Value {
    serde_json::json!({
        "id": format!("resp_sim_{custom_id}"),
        "object": "response",
        "created_at": unix_secs(created_ms),
        "model": SIM_MODEL,
        "status": "completed",
        "output": [{
            "type": "message",
            "id": format!("msg_sim_{custom_id}"),
            "role": "assistant",
            "content": [{
                "type": "output_text",
                "text": format!("sim-echo:{custom_id}"),
                "annotations": [],
            }],
        }],
        "usage": {
            "input_tokens": 0,
            "output_tokens": 0,
            "total_tokens": 0,
        },
        "sim_echo": echo_json(echo),
    })
}

pub fn gemini_skeleton(custom_id: &str, echo: &EchoData) -> Value {
    serde_json::json!({
        "candidates": [{
            "content": {
                "role": "model",
                "parts": [{ "text": format!("sim-echo:{custom_id}") }],
            },
            "finishReason": "STOP",
            "index": 0,
        }],
        "modelVersion": SIM_MODEL,
        "usageMetadata": {
            "promptTokenCount": 0,
            "candidatesTokenCount": 0,
            "totalTokenCount": 0,
        },
        "simEcho": echo_json(echo),
    })
}

pub fn embeddings_skeleton(echo: &EchoData) -> Value {
    serde_json::json!({
        "object": "list",
        "data": [{
            "object": "embedding",
            "index": 0,
            "embedding": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        }],
        "model": SIM_MODEL,
        "usage": {
            "prompt_tokens": 0,
            "total_tokens": 0,
        },
        "sim_echo": echo_json(echo),
    })
}

/// Style-driven body; provider-independent by design.
pub fn success_body(
    style: BodyStyle,
    custom_id: &str,
    echo: &EchoData,
    created_ms: u64,
) -> Value {
    match style {
        BodyStyle::ChatCompletions => chat_skeleton(custom_id, echo, created_ms),
        BodyStyle::Responses => responses_skeleton(custom_id, echo, created_ms),
        BodyStyle::GeminiNative => gemini_skeleton(custom_id, echo),
        BodyStyle::Embeddings => embeddings_skeleton(echo),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn rfc3339_known_dates() {
        assert_eq!(ms_to_rfc3339(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(ms_to_rfc3339(1_700_000_000_123), "2023-11-14T22:13:20.123Z");
        assert_eq!(ms_to_rfc3339(2_000_000_000_000), "2033-05-18T03:33:20.000Z");
        // leap day
        assert_eq!(ms_to_rfc3339(1_709_164_800_000), "2024-02-29T00:00:00.000Z");
    }

    #[test]
    fn echo_truncation_boundary() {
        let small = json!({"a": 1});
        let e = truncate_echo(&small);
        assert!(!e.truncated);
        assert_eq!(e.input, small);
        let big = json!({"blob": "x".repeat(ECHO_CAP_BYTES + 1)});
        let e = truncate_echo(&big);
        assert!(e.truncated);
        assert_eq!(e.input["sim_truncated"], json!(true));
        assert!(e.input["original_bytes"].as_u64().unwrap() > ECHO_CAP_BYTES as u64);
        assert!(serde_json::to_string(&e.input).unwrap().len() < ECHO_CAP_BYTES);
    }

    #[test]
    fn skeletons_carry_custom_id_and_zero_usage() {
        let echo = truncate_echo(&json!({"messages": []}));
        let chat = chat_skeleton("c1", &echo, 1_700_000_000_000);
        assert_eq!(chat["choices"][0]["message"]["content"], json!("sim-echo:c1"));
        assert_eq!(chat["usage"]["total_tokens"], json!(0));
        let resp = responses_skeleton("c2", &echo, 1_700_000_000_000);
        assert_eq!(
            resp["output"][0]["content"][0]["text"],
            json!("sim-echo:c2")
        );
        let gem = gemini_skeleton("c3", &echo);
        assert_eq!(
            gem["candidates"][0]["content"]["parts"][0]["text"],
            json!("sim-echo:c3")
        );
        let emb = embeddings_skeleton(&echo);
        assert_eq!(emb["object"], json!("list"));
        assert_eq!(emb["data"][0]["embedding"].as_array().unwrap().len(), 8);
    }
}
