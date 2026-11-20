//! OpenAI adapter: `/openai/v1/*`. File-based batches; JSONL results
//! split across output/error files.

use serde_json::Value;

use super::common::{
    CreateIntent, IngestOutcome, IngestRequest, ItemSource, RenderedFiles,
    as_obj, opt_str, req_obj, req_str, success_body, unix_secs,
};
use super::BatchProviderAdapter;
use crate::config::{error_file_id, output_file_id};
use crate::domain::{
    BatchStatus, ProviderKind, SimError, UnifiedBatch, UnifiedItem, UnifiedOutcome,
    sniff_body_style,
};

pub struct OpenAiAdapter;

pub const DEFAULT_ENDPOINT: &str = "/v1/chat/completions";

fn ingest_line(line: &str, number: usize) -> Result<UnifiedItem, SimError> {
    let ctx = format!("line {number}");
    let v: Value = serde_json::from_str(line)
        .map_err(|e| SimError::bad_request(format!("{ctx}: invalid JSON ({e})")))?;
    let obj = as_obj(&v, &ctx)?;
    let custom_id = req_str(obj, "custom_id", &ctx)?;
    let endpoint = opt_str(obj, "url").unwrap_or_else(|| DEFAULT_ENDPOINT.to_string());
    let body = req_obj(obj, "body", &ctx)?.clone();
    if obj.get("method").is_some_and(|m| !m.is_string()) {
        return Err(SimError::bad_request(format!("{ctx}: 'method' must be a string")));
    }
    Ok(UnifiedItem {
        custom_id,
        body_style: sniff_body_style(&endpoint, &body),
        endpoint,
        input_payload: body,
        outcome: None,
    })
}

fn status_name(s: BatchStatus) -> &'static str {
    match s {
        BatchStatus::Validating => "validating",
        BatchStatus::InProgress => "in_progress",
        BatchStatus::Completed => "completed",
        BatchStatus::Failed => "failed",
        BatchStatus::Cancelled => "cancelled",
    }
}

fn error_type_for(status: u16) -> &'static str {
    match status {
        401 => "authentication_error",
        429 => "rate_limit_error",
        500..=599 => "server_error",
        _ => "invalid_request_error",
    }
}

fn render_line(batch: &UnifiedBatch, item: &UnifiedItem) -> Value {
    match &item.outcome {
        Some(UnifiedOutcome::Success(echo)) => {
            let style = item.body_style.unwrap_or(crate::domain::BodyStyle::ChatCompletions);
            serde_json::json!({
                "custom_id": item.custom_id,
                "response": {
                    "status_code": 200,
                    "request_id": format!("req_sim_{}", item.custom_id),
                    "body": success_body(style, &item.custom_id, echo, batch.created_at_ms),
                },
                "error": null,
            })
        }
        Some(UnifiedOutcome::Error(e)) => serde_json::json!({
            "custom_id": item.custom_id,
            "response": null,
            "error": { "code": e.code, "message": e.message },
        }),
        None => serde_json::json!({
            "custom_id": item.custom_id,
            "response": null,
            "error": { "code": "pending", "message": "batch not complete" },
        }),
    }
}

fn terminal_files(batch: &UnifiedBatch, items: &[UnifiedItem]) -> (Option<String>, Option<String>) {
    if batch.status != BatchStatus::Completed {
        return (None, None);
    }
    let has_ok = items
        .iter()
        .any(|i| matches!(i.outcome, Some(UnifiedOutcome::Success(_))));
    let has_err = items
        .iter()
        .any(|i| matches!(i.outcome, Some(UnifiedOutcome::Error(_))));
    (
        has_ok.then(|| output_file_id(&batch.batch_id)),
        has_err.then(|| error_file_id(&batch.batch_id)),
    )
}

impl BatchProviderAdapter for OpenAiAdapter {
    fn ingest(&self, req: IngestRequest) -> Result<IngestOutcome, SimError> {
        match req {
            IngestRequest::CreateBody(body) => {
                let obj = as_obj(body, "batch")?;
                let input_file_id = req_str(obj, "input_file_id", "batch")?;
                let endpoint = opt_str(obj, "endpoint").unwrap_or_else(|| DEFAULT_ENDPOINT.to_string());
                Ok(IngestOutcome::CreateIntent(CreateIntent {
                    source: ItemSource::FileRef(input_file_id),
                    callback_url: None,
                    model: None,
                    endpoint: Some(endpoint),
                }))
            }
            IngestRequest::FileLine { line, number } => {
                ingest_line(line, number).map(IngestOutcome::Item)
            }
            IngestRequest::InlineUnit { .. } => Err(SimError::bad_request(
                "openai batches require an input_file_id; inline requests unsupported",
            )),
        }
    }

    fn render_status(&self, batch: &UnifiedBatch, items: &[UnifiedItem]) -> Value {
        let (output_file_id, error_file_id) = terminal_files(batch, items);
        let at = |ms: Option<u64>| ms.map(unix_secs).map(Value::from).unwrap_or(Value::Null);
        let beyond_validating = !matches!(batch.status, BatchStatus::Validating);
        let completed = (batch.status == BatchStatus::Completed).then(|| batch.completed_at_ms);
        let failed = (batch.status == BatchStatus::Failed).then(|| batch.completed_at_ms);
        let cancelled = (batch.status == BatchStatus::Cancelled).then(|| batch.completed_at_ms);
        let errors: Vec<Value> = batch
            .batch_error
            .as_ref()
            .map(|e| {
                vec![serde_json::json!({
                    "code": e.code, "message": e.message,
                    "param": null, "line": null,
                })]
            })
            .unwrap_or_default();
        serde_json::json!({
            "id": batch.batch_id,
            "object": "batch",
            "endpoint": batch.endpoint.as_deref().unwrap_or(DEFAULT_ENDPOINT),
            "errors": { "object": "list", "data": errors },
            "input_file_id": batch.input_file_id,
            "completion_window": "24h",
            "status": status_name(batch.status),
            "output_file_id": output_file_id,
            "error_file_id": error_file_id,
            "created_at": unix_secs(batch.created_at_ms),
            "in_progress_at": if beyond_validating { at(Some(batch.created_at_ms)) } else { Value::Null },
            "expires_at": null,
            "finalizing_at": null,
            "completed_at": at(completed.flatten()),
            "failed_at": at(failed.flatten()),
            "expired_at": null,
            "cancelling_at": null,
            "cancelled_at": at(cancelled.flatten()),
            "request_counts": {
                "total": batch.counts.total,
                "completed": batch.counts.completed,
                "failed": batch.counts.failed,
            },
            "metadata": {},
        })
    }

    fn render_results(&self, batch: &UnifiedBatch, items: &[UnifiedItem]) -> RenderedFiles {
        let mut ok_lines = Vec::new();
        let mut err_lines = Vec::new();
        for item in items {
            let line = serde_json::to_string(&render_line(batch, item)).unwrap_or_default();
            match item.outcome {
                Some(UnifiedOutcome::Error(_)) => err_lines.push(line),
                _ => ok_lines.push(line),
            }
        }
        RenderedFiles::jsonl(
            ok_lines.join("\n"),
            (!err_lines.is_empty()).then(|| err_lines.join("\n")),
        )
    }

    fn render_item(&self, batch: &UnifiedBatch, item: &UnifiedItem) -> Value {
        render_line(batch, item)
    }

    fn build_webhook(&self, batch: &UnifiedBatch, _items: &[UnifiedItem]) -> Value {
        // Simulator extension: OpenAI has no native batch webhook.
        let event = match batch.status {
            BatchStatus::Completed => "batch.completed",
            BatchStatus::Failed => "batch.failed",
            BatchStatus::Cancelled => "batch.cancelled",
            _ => "batch.updated",
        };
        serde_json::json!({
            "id": format!("evt_{}", batch.batch_id),
            "object": "event",
            "type": event,
            "created": unix_secs(batch.updated_at_ms),
            "data": self.render_status(batch, &[]),
        })
    }

    fn render_error(&self, err: &SimError) -> Value {
        serde_json::json!({
            "error": {
                "message": err.message,
                "type": error_type_for(err.http_status),
                "param": null,
                "code": err.code,
            }
        })
    }

    fn create_http_status(&self) -> u16 {
        200
    }
}

#[allow(dead_code)]
fn provider_kind() -> ProviderKind {
    ProviderKind::OpenAI
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{EchoData, RequestCounts};
    use serde_json::json;

    fn batch(status: BatchStatus) -> UnifiedBatch {
        UnifiedBatch {
            batch_id: "batch_sim1".to_string(),
            provider: ProviderKind::OpenAI,
            status,
            created_at_ms: 1_700_000_000_000,
            updated_at_ms: 1_700_000_001_000,
            completed_at_ms: Some(1_700_000_002_000),
            chaos: crate::domain::ChaosConfig {
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
            webhook_status: crate::domain::WebhookStatus::Skipped,
            input_file_id: Some("file_abc".to_string()),
            counts: RequestCounts { total: 1, completed: 1, failed: 0 },
            batch_error: None,
            model: None,
            endpoint: Some(DEFAULT_ENDPOINT.to_string()),
            poll_count: 0,
            inline_items: vec![],
        }
    }

    #[test]
    fn line_round_trip() {
        let a = OpenAiAdapter;
        let line = r#"{"custom_id":"c1","method":"POST","url":"/v1/chat/completions","body":{"model":"m","messages":[]}}"#;
        let item = match a
            .ingest(IngestRequest::FileLine { line, number: 1 })
            .unwrap()
        {
            IngestOutcome::Item(i) => i,
            IngestOutcome::CreateIntent(_) => panic!("expected item"),
        };
        assert_eq!(item.custom_id, "c1");
        // bad json + missing custom_id → 400 naming the line
        assert!(a.ingest(IngestRequest::FileLine { line: "{", number: 7 }).is_err());
        let err = a
            .ingest(IngestRequest::FileLine {
                line: r#"{"body":{}}"#,
                number: 3,
            })
            .unwrap_err();
        assert_eq!(err.http_status, 400);
        assert!(err.message.contains("line 3"));
    }

    #[test]
    fn status_and_files() {
        let a = OpenAiAdapter;
        let b = batch(BatchStatus::Completed);
        let items = vec![UnifiedItem {
            custom_id: "c1".to_string(),
            endpoint: DEFAULT_ENDPOINT.to_string(),
            body_style: Some(crate::domain::BodyStyle::ChatCompletions),
            input_payload: json!({}),
            outcome: Some(UnifiedOutcome::Success(EchoData {
                input: json!({}),
                truncated: false,
            })),
        }];
        let s = a.render_status(&b, &items);
        assert_eq!(s["object"], json!("batch"));
        assert_eq!(s["status"], json!("completed"));
        assert_eq!(s["output_file_id"], json!("file-out-batch_sim1"));
        assert_eq!(s["error_file_id"], json!(null));
        assert_eq!(s["created_at"], json!(1_700_000_000));
        let files = a.render_results(&b, &items);
        match files.primary {
            crate::adapters::common::RenderedResults::Jsonl(text) => {
                let v: Value = serde_json::from_str(text.lines().next().unwrap()).unwrap();
                assert_eq!(v["response"]["status_code"], json!(200));
                assert_eq!(v["response"]["body"]["object"], json!("chat.completion"));
            }
            _ => panic!("expected jsonl"),
        }
        assert!(files.secondary.is_none());
    }

    #[test]
    fn error_envelope() {
        let a = OpenAiAdapter;
        let v = a.render_error(&SimError::not_found("batch 'x'"));
        assert_eq!(v["error"]["type"], json!("invalid_request_error"));
    }
}
