//! Gemini adapter: `/gemini/v1beta/*`. `batchGenerateContent` with inline
//! or file inputs; Long-Running-Operation status envelope; `inlinedResponses`
//! vs `responsesFile` switched by serialized size (256 KiB, plan-locked).
//! Shapes per ai.google.dev batch API guide.

use serde_json::Value;

use super::common::{
    CreateIntent, IngestOutcome, IngestRequest, ItemSource, RenderedFiles, as_obj,
    ms_to_rfc3339, req_obj, success_body,
};
use super::BatchProviderAdapter;
use crate::config::GEMINI_INLINE_LIMIT_BYTES;
use crate::domain::{
    BatchStatus, SimError, UnifiedBatch, UnifiedItem, UnifiedOutcome, sniff_body_style,
};

pub struct GeminiAdapter;

/// Sim-faithful LRO metadata type (real type string undocumented for batches).
pub const META_TYPE: &str =
    "type.googleapis.com/google.ai.generativelanguage.v1beta.BatchOperationMetadata";

/// File-mode input lines carry no endpoint; payload shape drives the sniff so
/// OpenAI-compatible `{messages}` lines render chat skeletons.
pub const FILE_ENDPOINT: &str = "";

fn job_state(s: BatchStatus) -> &'static str {
    match s {
        BatchStatus::Validating => "JOB_STATE_PENDING",
        BatchStatus::InProgress => "JOB_STATE_RUNNING",
        BatchStatus::Completed => "JOB_STATE_SUCCEEDED",
        BatchStatus::Failed => "JOB_STATE_FAILED",
        BatchStatus::Cancelled => "JOB_STATE_CANCELLED",
    }
}

pub fn rpc_status_name(http_status: u16) -> &'static str {
    match http_status {
        400 => "INVALID_ARGUMENT",
        401 => "UNAUTHENTICATED",
        404 => "NOT_FOUND",
        409 => "FAILED_PRECONDITION",
        429 => "RESOURCE_EXHAUSTED",
        500 => "INTERNAL",
        503 => "UNAVAILABLE",
        _ => "UNKNOWN",
    }
}

fn rpc_error(e: &SimError) -> Value {
    serde_json::json!({
        "code": e.http_status,
        "message": e.message,
        "status": rpc_status_name(e.http_status),
    })
}

fn ingest_request_unit(request: &Value, key: &str) -> Result<UnifiedItem, SimError> {
    let obj = as_obj(request, "request")?;
    let payload = Value::Object(obj.clone());
    Ok(UnifiedItem {
        custom_id: key.to_string(),
        body_style: sniff_body_style(FILE_ENDPOINT, &payload),
        endpoint: FILE_ENDPOINT.to_string(),
        input_payload: payload,
        outcome: None,
    })
}

fn render_inlined(batch: &UnifiedBatch, item: &UnifiedItem) -> Value {
    match &item.outcome {
        Some(UnifiedOutcome::Success(echo)) => {
            let style = item.body_style.unwrap_or(crate::domain::BodyStyle::GeminiNative);
            serde_json::json!({
                "metadata": { "key": item.custom_id },
                "response": success_body(style, &item.custom_id, echo, batch.created_at_ms),
            })
        }
        Some(UnifiedOutcome::Error(e)) => serde_json::json!({
            "metadata": { "key": item.custom_id },
            "error": rpc_error(e),
        }),
        None => serde_json::json!({
            "metadata": { "key": item.custom_id },
            "error": { "code": 400, "message": "batch not complete", "status": "FAILED_PRECONDITION" },
        }),
    }
}

fn render_file_line(batch: &UnifiedBatch, item: &UnifiedItem) -> Value {
    match &item.outcome {
        Some(UnifiedOutcome::Success(echo)) => {
            let style = item.body_style.unwrap_or(crate::domain::BodyStyle::GeminiNative);
            serde_json::json!({
                "key": item.custom_id,
                "response": success_body(style, &item.custom_id, echo, batch.created_at_ms),
            })
        }
        Some(UnifiedOutcome::Error(e)) => serde_json::json!({
            "key": item.custom_id,
            "error": rpc_error(e),
        }),
        None => serde_json::json!({
            "key": item.custom_id,
            "error": { "code": 400, "message": "batch not complete", "status": "FAILED_PRECONDITION" },
        }),
    }
}

fn inlined_docs(batch: &UnifiedBatch, items: &[UnifiedItem]) -> Vec<Value> {
    items.iter().map(|i| render_inlined(batch, i)).collect()
}

/// Poll envelope shared by `GET batches/{id}` and the `operations/{id}` alias.
fn operation(batch: &UnifiedBatch, items: &[UnifiedItem]) -> Value {
    let done = batch.status.is_terminal();
    let mut op = serde_json::json!({
        "name": format!("batches/{}", batch.batch_id),
        "metadata": {
            "@type": META_TYPE,
            "state": job_state(batch.status),
            "createTime": ms_to_rfc3339(batch.created_at_ms),
        },
        "done": done,
    });
    if let BatchStatus::Failed = batch.status
        && let Some(e) = &batch.batch_error
    {
        op["error"] = rpc_error(e);
        return op;
    }
    if batch.status == BatchStatus::Completed {
        let docs = inlined_docs(batch, items);
        let bytes = serde_json::to_string(&docs).map(|s| s.len()).unwrap_or(usize::MAX);
        if bytes <= GEMINI_INLINE_LIMIT_BYTES {
            op["response"] = serde_json::json!({ "inlinedResponses": docs });
        } else {
            op["response"] = serde_json::json!({
                "responsesFile": format!("files/{}", crate::config::output_file_id(&batch.batch_id)),
            });
        }
    }
    op
}

impl BatchProviderAdapter for GeminiAdapter {
    fn ingest(&self, req: IngestRequest) -> Result<IngestOutcome, SimError> {
        match req {
            IngestRequest::CreateBody(body) => {
                let root = as_obj(body, "batchGenerateContent")?;
                let batch_obj = root
                    .get("batch")
                    .and_then(|v| v.as_object())
                    .ok_or_else(|| SimError::bad_request("batchGenerateContent: 'batch' is required"))?;
                let input = batch_obj
                    .get("input_config")
                    .and_then(|v| v.as_object())
                    .ok_or_else(|| {
                        SimError::bad_request("batch: 'input_config' is required")
                    })?;
                let has_requests = input.contains_key("requests");
                let has_file = input.contains_key("file_name");
                match (has_requests, has_file) {
                    (true, true) => {
                        return Err(SimError::bad_request(
                            "input_config: exactly one of 'requests' or 'file_name'",
                        ));
                    }
                    (false, false) => {
                        return Err(SimError::bad_request(
                            "input_config: one of 'requests' or 'file_name' is required",
                        ));
                    }
                    (false, true) => {
                        let name = input
                            .get("file_name")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        let file_id = name.strip_prefix("files/").ok_or_else(|| {
                            SimError::bad_request(
                                "input_config: 'file_name' must look like 'files/{id}'",
                            )
                        })?;
                        if file_id.is_empty() {
                            return Err(SimError::bad_request(
                                "input_config: 'file_name' must look like 'files/{id}'",
                            ));
                        }
                        return Ok(IngestOutcome::CreateIntent(CreateIntent {
                            source: ItemSource::FileRef(file_id.to_string()),
                            callback_url: None,
                            model: None,
                            endpoint: None,
                        }));
                    }
                    (true, false) => {}
                }
                let inner = input
                    .get("requests")
                    .and_then(|v| v.as_object())
                    .and_then(|o| o.get("requests"))
                    .and_then(|v| v.as_array())
                    .ok_or_else(|| {
                        SimError::bad_request("input_config: 'requests.requests[]' is required")
                    })?;
                if inner.is_empty() {
                    return Err(SimError::bad_request(
                        "input_config: 'requests.requests[]' must not be empty",
                    ));
                }
                let mut items = Vec::with_capacity(inner.len());
                for (i, unit) in inner.iter().enumerate() {
                    let ctx = format!("requests[{i}]");
                    let u = as_obj(unit, &ctx)?;
                    let request = req_obj(u, "request", &ctx)?.clone();
                    let key = u
                        .get("metadata")
                        .and_then(|m| m.as_object())
                        .and_then(|m| m.get("key"))
                        .and_then(|k| k.as_str())
                        .ok_or_else(|| {
                            SimError::bad_request(format!("{ctx}: 'metadata.key' is required"))
                        })?;
                    if key.is_empty() {
                        return Err(SimError::bad_request(format!(
                            "{ctx}: 'metadata.key' must not be empty"
                        )));
                    }
                    items.push(ingest_request_unit(&request, key)?);
                }
                Ok(IngestOutcome::CreateIntent(CreateIntent {
                    source: ItemSource::Inline(items),
                    callback_url: None,
                    model: None,
                    endpoint: None,
                }))
            }
            IngestRequest::InlineUnit { unit, .. } => {
                let u = as_obj(unit, "inline")?;
                let key = u
                    .get("key")
                    .and_then(|k| k.as_str())
                    .ok_or_else(|| SimError::bad_request("inline: 'key' is required"))?;
                ingest_request_unit(req_obj(u, "request", "inline")?, key)
                    .map(IngestOutcome::Item)
            }
            IngestRequest::FileLine { line, number } => {
                let ctx = format!("line {number}");
                let v: Value = serde_json::from_str(line)
                    .map_err(|e| SimError::bad_request(format!("{ctx}: invalid JSON ({e})")))?;
                let obj = as_obj(&v, &ctx)?;
                let key = obj
                    .get("key")
                    .and_then(|k| k.as_str())
                    .ok_or_else(|| SimError::bad_request(format!("{ctx}: 'key' is required")))?;
                if key.is_empty() {
                    return Err(SimError::bad_request(format!("{ctx}: 'key' must not be empty")));
                }
                ingest_request_unit(req_obj(obj, "request", &ctx)?, key)
                    .map(IngestOutcome::Item)
            }
        }
    }

    fn render_status(&self, batch: &UnifiedBatch, items: &[UnifiedItem]) -> Value {
        operation(batch, items)
    }

    fn render_results(&self, batch: &UnifiedBatch, items: &[UnifiedItem]) -> RenderedFiles {
        let docs = inlined_docs(batch, items);
        let bytes = serde_json::to_string(&docs).map(|s| s.len()).unwrap_or(usize::MAX);
        if bytes <= GEMINI_INLINE_LIMIT_BYTES {
            RenderedFiles::json(serde_json::json!({ "inlinedResponses": docs }))
        } else {
            let text = items
                .iter()
                .map(|i| serde_json::to_string(&render_file_line(batch, i)).unwrap_or_default())
                .collect::<Vec<_>>()
                .join("\n");
            RenderedFiles::jsonl(text, None)
        }
    }

    fn render_item(&self, batch: &UnifiedBatch, item: &UnifiedItem) -> Value {
        render_inlined(batch, item)
    }

    fn build_webhook(&self, batch: &UnifiedBatch, items: &[UnifiedItem]) -> Value {
        // Simulator extension: operation envelope plus event marker.
        let event = match batch.status {
            BatchStatus::Completed => "batch.completed",
            BatchStatus::Failed => "batch.failed",
            BatchStatus::Cancelled => "batch.cancelled",
            _ => "batch.updated",
        };
        let mut op = operation(batch, items);
        if let Value::Object(m) = &mut op {
            m.insert("simEvent".to_string(), Value::String(event.to_string()));
        }
        op
    }

    fn render_error(&self, err: &SimError) -> Value {
        serde_json::json!({ "error": {
            "code": self.error_http_status(err),
            "message": err.message,
            "status": rpc_status_name(err.http_status),
        }})
    }

    fn create_http_status(&self) -> u16 {
        200
    }

    fn error_http_status(&self, err: &SimError) -> u16 {
        if err.http_status == 409 { 400 } else { err.http_status }
    }
}

/// File API upload response (`POST /upload/v1beta/files`).
pub fn render_file_object(file_id: &str, size_bytes: usize, now_ms: u64) -> Value {
    serde_json::json!({
        "file": {
            "name": format!("files/{file_id}"),
            "displayName": file_id,
            "mimeType": "application/jsonl",
            "sizeBytes": size_bytes.to_string(),
            "createTime": ms_to_rfc3339(now_ms),
            "state": "ACTIVE",
        }
    })
}

/// Extract the JSONL payload from a raw body or a `multipart/related` upload.
/// Raw bodies pass through; multipart scans parts for the first JSON-ish body.
pub fn extract_upload_payload(content_type: Option<&str>, body: &[u8]) -> Result<Vec<u8>, SimError> {
    let ct = content_type.unwrap_or("");
    if !ct.to_lowercase().starts_with("multipart/") {
        return Ok(body.to_vec());
    }
    let boundary = ct
        .split(';')
        .find_map(|p| {
            let p = p.trim();
            p.strip_prefix("boundary=")
                .or_else(|| p.strip_prefix("Boundary="))
                .map(|b| b.trim_matches('"').to_string())
        })
        .ok_or_else(|| SimError::bad_request("upload: multipart missing boundary"))?;
    let sep = format!("--{boundary}");
    let text = std::str::from_utf8(body)
        .map_err(|_| SimError::bad_request("upload: body must be UTF-8"))?;
    for part in text.split(&sep) {
        let part = part.trim().trim_end_matches("--").trim();
        if part.is_empty() {
            continue;
        }
        if let Some(idx) = part.find("\r\n\r\n").or_else(|| part.find("\n\n")) {
            let body_start = if part[idx..].starts_with("\r\n") { idx + 4 } else { idx + 2 };
            let payload = part[body_start..].trim();
            if payload.starts_with('{') {
                return Ok(payload.as_bytes().to_vec());
            }
        }
    }
    Err(SimError::bad_request("upload: no JSON part found"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{ChaosConfig, EchoData, ProviderKind, RequestCounts, WebhookStatus};
    use serde_json::json;

    fn batch(status: BatchStatus) -> UnifiedBatch {
        UnifiedBatch {
            batch_id: "batch_g1".to_string(),
            provider: ProviderKind::Gemini,
            status,
            created_at_ms: 1_700_000_000_000,
            updated_at_ms: 1_700_000_001_000,
            completed_at_ms: Some(1_700_000_002_000),
            chaos: ChaosConfig {
                validation_delay_ms: 0,
                processing_delay_ms: 0,
                jitter_pct: 0,
                batch_fail_rate: 0.0,
                item_fail_rate: 0.0,
                edge_chaos_rate: 0.0,
                seed: 1,
            },
            callback_url: None,
            webhook_secret: None,
            webhook_status: WebhookStatus::Skipped,
            input_file_id: None,
            counts: RequestCounts { total: 1, completed: 1, failed: 0 },
            batch_error: None,
            model: Some("models/gemini-4-hydrogen".to_string()),
            endpoint: None,
            poll_count: 0,
            inline_items: vec![],
        }
    }

    fn ok_item() -> UnifiedItem {
        UnifiedItem {
            custom_id: "request-1".to_string(),
            endpoint: String::new(),
            body_style: Some(crate::domain::BodyStyle::GeminiNative),
            input_payload: json!({"contents": []}),
            outcome: Some(UnifiedOutcome::Success(EchoData {
                input: json!({"contents": []}),
                truncated: false,
            })),
        }
    }

    #[test]
    fn inline_create_body_shape() {
        let a = GeminiAdapter;
        let body = json!({
            "batch": {
                "display_name": "my-batch-requests",
                "input_config": { "requests": { "requests": [
                    {"request": {"contents": [{"parts": [{"text": "hi"}]}]},
                     "metadata": {"key": "request-1"}},
                ]}},
            }
        });
        match a.ingest(IngestRequest::CreateBody(&body)).unwrap() {
            IngestOutcome::CreateIntent(intent) => match intent.source {
                ItemSource::Inline(items) => {
                    assert_eq!(items.len(), 1);
                    assert_eq!(items[0].custom_id, "request-1");
                    assert_eq!(
                        items[0].body_style,
                        Some(crate::domain::BodyStyle::GeminiNative)
                    );
                }
                ItemSource::FileRef(_) => panic!("expected inline"),
            },
            IngestOutcome::Item(_) => panic!("expected intent"),
        }
        // file variant
        let body = json!({"batch": {"input_config": {"file_name": "files/file_x"}}});
        match a.ingest(IngestRequest::CreateBody(&body)).unwrap() {
            IngestOutcome::CreateIntent(intent) => {
                assert!(matches!(intent.source, ItemSource::FileRef(f) if f == "file_x"));
            }
            _ => panic!("expected intent"),
        }
        // both → 400; bad file_name → 400
        assert!(
            a.ingest(IngestRequest::CreateBody(&json!({"batch": {"input_config": {
                "requests": {"requests": []}, "file_name": "files/x" }}})))
            .is_err()
        );
        assert!(
            a.ingest(IngestRequest::CreateBody(
                &json!({"batch": {"input_config": {"file_name": "nope"}}})
            ))
            .is_err()
        );
    }

    #[test]
    fn openai_compat_line_sniffs_chat() {
        let a = GeminiAdapter;
        let line = r#"{"key":"k1","request":{"model":"m","messages":[{"role":"user","content":"hi"}]}}"#;
        match a.ingest(IngestRequest::FileLine { line, number: 1 }).unwrap() {
            IngestOutcome::Item(i) => assert_eq!(
                i.body_style,
                Some(crate::domain::BodyStyle::ChatCompletions)
            ),
            _ => panic!("expected item"),
        }
    }

    #[test]
    fn operation_envelope_and_threshold() {
        let a = GeminiAdapter;
        let item = ok_item();
        let pending = a.render_status(&batch(BatchStatus::InProgress), &[item.clone()]);
        assert_eq!(pending["done"], json!(false));
        assert_eq!(pending["metadata"]["state"], json!("JOB_STATE_RUNNING"));
        assert_eq!(pending["name"], json!("batches/batch_g1"));
        assert!(pending.get("response").is_none());
        let done = a.render_status(&batch(BatchStatus::Completed), &[item]);
        assert_eq!(done["done"], json!(true));
        assert_eq!(
            done["response"]["inlinedResponses"][0]["metadata"]["key"],
            json!("request-1")
        );
        // large batch flips to responsesFile, identically in status + results
        let big = UnifiedItem {
            input_payload: json!({"contents": []}),
            outcome: Some(UnifiedOutcome::Success(EchoData {
                input: json!({"contents": [{"parts": [{"text": "y".repeat(300_000)}]}]}),
                truncated: false,
            })),
            ..ok_item()
        };
        let done = a.render_status(&batch(BatchStatus::Completed), std::slice::from_ref(&big));
        assert_eq!(
            done["response"]["responsesFile"],
            json!("files/file-out-batch_g1")
        );
    }

    #[test]
    fn multipart_extraction() {
        let raw = b"{\"key\":\"a\"}\n";
        assert_eq!(
            extract_upload_payload(None, raw).unwrap(),
            raw.to_vec()
        );
        let mp = b"--b1\r\nContent-Type: application/json\r\n\r\n{\"key\":\"a\"}\r\n--b1--";
        assert_eq!(
            extract_upload_payload(Some("multipart/related; boundary=b1"), mp).unwrap(),
            b"{\"key\":\"a\"}".to_vec()
        );
        assert!(extract_upload_payload(Some("multipart/related"), mp).is_err());
    }

    #[test]
    fn conflict_maps_to_400_failed_precondition() {
        use super::super::BatchProviderAdapter;
        let a = GeminiAdapter;
        let err = SimError::conflict("already 'completed'");
        assert_eq!(a.error_http_status(&err), 400);
        let v = a.render_error(&err);
        assert_eq!(v["error"]["code"], json!(400));
        assert_eq!(v["error"]["status"], json!("FAILED_PRECONDITION"));
        let nf = SimError::not_found("batch 'x'");
        assert_eq!(a.error_http_status(&nf), 404);
        assert_eq!(a.render_error(&nf)["error"]["status"], json!("NOT_FOUND"));
    }
}
