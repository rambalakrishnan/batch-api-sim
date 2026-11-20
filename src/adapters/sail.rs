//! Sail Research adapter: `/sail/v1/*`. Inline `requests` with `params`
//! payloads (strict: `body` rejected), inline results, per-item lookup.
//! Sail is simulator-defined per the SAD (no public wire docs to mirror).

use serde_json::Value;

use super::common::{
    CreateIntent, IngestOutcome, IngestRequest, ItemSource, RenderedFiles, as_obj,
    opt_str, req_obj, req_str, success_body, unix_secs,
};
use super::BatchProviderAdapter;
use crate::domain::{
    BatchStatus, SimError, UnifiedBatch, UnifiedItem, UnifiedOutcome, sniff_body_style,
};

pub struct SailAdapter;

pub const DEFAULT_ENDPOINT: &str = "/v1/responses";

fn status_name(s: BatchStatus) -> &'static str {
    match s {
        BatchStatus::Validating => "validating",
        BatchStatus::InProgress => "in_progress",
        BatchStatus::Completed => "completed",
        BatchStatus::Failed => "failed",
        BatchStatus::Cancelled => "cancelled",
    }
}

fn ingest_unit(unit: &Value, endpoint: &str) -> Result<UnifiedItem, SimError> {
    let obj = as_obj(unit, "requests[]")?;
    if obj.contains_key("body") {
        return Err(SimError::bad_request(
            "requests[]: sail units use 'params', not 'body'",
        ));
    }
    let custom_id = req_str(obj, "custom_id", "requests[]")?;
    let params = req_obj(obj, "params", "requests[]")?.clone();
    Ok(UnifiedItem {
        custom_id,
        body_style: sniff_body_style(endpoint, &params),
        endpoint: endpoint.to_string(),
        input_payload: params,
        outcome: None,
    })
}

fn render_result(batch: &UnifiedBatch, item: &UnifiedItem) -> Value {
    match &item.outcome {
        Some(UnifiedOutcome::Success(echo)) => {
            let style = item.body_style.unwrap_or(crate::domain::BodyStyle::ChatCompletions);
            serde_json::json!({
                "custom_id": item.custom_id,
                "response": {
                    "status_code": 200,
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

impl BatchProviderAdapter for SailAdapter {
    fn ingest(&self, req: IngestRequest) -> Result<IngestOutcome, SimError> {
        match req {
            IngestRequest::CreateBody(body) => {
                let obj = as_obj(body, "batch")?;
                let endpoint = opt_str(obj, "endpoint").unwrap_or_else(|| DEFAULT_ENDPOINT.to_string());
                let requests = obj.get("requests").and_then(|v| v.as_array()).ok_or_else(|| {
                    SimError::bad_request("batch: 'requests' array is required")
                })?;
                if requests.is_empty() {
                    return Err(SimError::bad_request("batch: 'requests' must not be empty"));
                }
                let mut items = Vec::with_capacity(requests.len());
                for unit in requests {
                    items.push(ingest_unit(unit, &endpoint)?);
                }
                Ok(IngestOutcome::CreateIntent(CreateIntent {
                    source: ItemSource::Inline(items),
                    callback_url: None,
                    model: opt_str(obj, "model"),
                    endpoint: Some(endpoint),
                }))
            }
            IngestRequest::InlineUnit { unit, endpoint } => {
                ingest_unit(unit, endpoint).map(IngestOutcome::Item)
            }
            IngestRequest::FileLine { .. } => Err(SimError::bad_request(
                "sail batches are inline-only; file inputs unsupported",
            )),
        }
    }

    fn render_status(&self, batch: &UnifiedBatch, items: &[UnifiedItem]) -> Value {
        let results = batch.status.is_terminal().then(|| {
            items
                .iter()
                .map(|i| render_result(batch, i))
                .collect::<Vec<_>>()
        });
        serde_json::json!({
            "id": batch.batch_id,
            "object": "batch",
            "endpoint": batch.endpoint.as_deref().unwrap_or(DEFAULT_ENDPOINT),
            "status": status_name(batch.status),
            "created_at": unix_secs(batch.created_at_ms),
            "completed_at": batch.completed_at_ms.map(unix_secs),
            "request_counts": {
                "total": batch.counts.total,
                "completed": batch.counts.completed,
                "failed": batch.counts.failed,
            },
            "error": batch.batch_error.as_ref().map(|e| serde_json::json!({
                "code": e.code, "message": e.message,
            })),
            "results": results,
        })
    }

    fn render_results(&self, batch: &UnifiedBatch, items: &[UnifiedItem]) -> RenderedFiles {
        RenderedFiles::json(Value::Array(
            items.iter().map(|i| render_result(batch, i)).collect(),
        ))
    }

    fn render_item(&self, batch: &UnifiedBatch, item: &UnifiedItem) -> Value {
        render_result(batch, item)
    }

    fn build_webhook(&self, batch: &UnifiedBatch, items: &[UnifiedItem]) -> Value {
        // Simulator extension (OpenAI-style event envelope).
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
            "data": self.render_status(batch, items),
        })
    }

    fn render_error(&self, err: &SimError) -> Value {
        serde_json::json!({
            "error": {
                "message": err.message,
                "type": "invalid_request_error",
                "param": null,
                "code": err.code,
            }
        })
    }

    fn create_http_status(&self) -> u16 {
        200
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{ChaosConfig, ProviderKind, RequestCounts, WebhookStatus};
    use serde_json::json;

    #[test]
    fn params_enforced_body_rejected() {
        let a = SailAdapter;
        let ok = json!({
            "requests": [{"custom_id": "a", "params": {"input": []}}],
        });
        assert!(matches!(
            a.ingest(IngestRequest::CreateBody(&ok)).unwrap(),
            IngestOutcome::CreateIntent(_)
        ));
        let bad = json!({
            "requests": [{"custom_id": "a", "body": {}}],
        });
        let err = a.ingest(IngestRequest::CreateBody(&bad)).unwrap_err();
        assert_eq!(err.http_status, 400);
        assert!(err.message.contains("params"));
    }

    #[test]
    fn status_shape() {
        let a = SailAdapter;
        let batch = UnifiedBatch {
            batch_id: "batch_s1".to_string(),
            provider: ProviderKind::Sail,
            status: BatchStatus::Completed,
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
            counts: RequestCounts { total: 0, completed: 0, failed: 0 },
            batch_error: None,
            model: None,
            endpoint: None,
            poll_count: 0,
            inline_items: vec![],
        };
        let s = a.render_status(&batch, &[]);
        assert_eq!(s["endpoint"], json!("/v1/responses"));
        assert_eq!(s["results"], json!([]));
        assert_eq!(s["completed_at"], json!(1_700_000_002));
    }
}
