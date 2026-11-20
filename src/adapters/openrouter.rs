//! OpenRouter adapter: `/openrouter/api/v1/*`. Inline `requests` batches,
//! inline `results`, native `callback_url` webhooks.
//! Shapes per openrouter.ai batch API reference (BatchSubmitBody/BatchObject).

use serde_json::Value;

use super::common::{
    CreateIntent, IngestOutcome, IngestRequest, ItemSource, RenderedFiles, as_obj,
    opt_str, req_obj, req_str, success_body, unix_secs,
};
use super::BatchProviderAdapter;
use crate::domain::{
    BatchStatus, SimError, UnifiedBatch, UnifiedItem, UnifiedOutcome, sniff_body_style,
};

pub struct OpenRouterAdapter;

pub const ENDPOINTS: [&str; 5] = [
    "/v1/chat/completions",
    "/v1/responses",
    "/v1/messages",
    "/v1/embeddings",
    "/v1/audio/speech",
];

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
    let custom_id = req_str(obj, "custom_id", "requests[]")?;
    let body = req_obj(obj, "body", "requests[]")?.clone();
    Ok(UnifiedItem {
        custom_id,
        body_style: sniff_body_style(endpoint, &body),
        endpoint: endpoint.to_string(),
        input_payload: body,
        outcome: None,
    })
}

fn render_result(batch: &UnifiedBatch, item: &UnifiedItem) -> Value {
    match &item.outcome {
        Some(UnifiedOutcome::Success(echo)) => {
            let style = item.body_style.unwrap_or(crate::domain::BodyStyle::ChatCompletions);
            serde_json::json!({
                "id": format!("res_{}", item.custom_id),
                "custom_id": item.custom_id,
                "response": {
                    "body": success_body(style, &item.custom_id, echo, batch.created_at_ms),
                },
                "error": null,
            })
        }
        Some(UnifiedOutcome::Error(e)) => {
            let err_type = if e.http_status == 429 {
                "rate_limit_error"
            } else {
                "api_error"
            };
            serde_json::json!({
                "id": format!("res_{}", item.custom_id),
                "custom_id": item.custom_id,
                "response": null,
                "error": { "type": err_type, "message": e.message, "param": null },
            })
        }
        None => serde_json::json!({
            "id": format!("res_{}", item.custom_id),
            "custom_id": item.custom_id,
            "response": null,
            "error": { "type": "pending_error", "message": "batch not complete", "param": null },
        }),
    }
}

impl BatchProviderAdapter for OpenRouterAdapter {
    fn ingest(&self, req: IngestRequest) -> Result<IngestOutcome, SimError> {
        match req {
            IngestRequest::CreateBody(body) => {
                let obj = as_obj(body, "batch")?;
                let endpoint = req_str(obj, "endpoint", "batch")?;
                if !ENDPOINTS.contains(&endpoint.as_str()) {
                    return Err(SimError::bad_request(format!(
                        "batch: unknown endpoint '{endpoint}'"
                    )));
                }
                let model = req_str(obj, "model", "batch")?;
                if obj.contains_key("input") {
                    return Err(SimError::bad_request(
                        "batch: file/BigQuery 'input' unsupported; use inline 'requests'",
                    ));
                }
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
                let callback_url = opt_str(obj, "callback_url");
                if let Some(url) = &callback_url
                    && !url.starts_with("https://")
                {
                    return Err(SimError::bad_request(
                        "batch: 'callback_url' must be an https URL",
                    ));
                }
                Ok(IngestOutcome::CreateIntent(CreateIntent {
                    source: ItemSource::Inline(items),
                    callback_url,
                    model: Some(model),
                    endpoint: Some(endpoint),
                }))
            }
            IngestRequest::InlineUnit { unit, endpoint } => {
                ingest_unit(unit, endpoint).map(IngestOutcome::Item)
            }
            IngestRequest::FileLine { .. } => Err(SimError::bad_request(
                "openrouter batches are inline-only; file inputs unsupported",
            )),
        }
    }

    fn render_status(&self, batch: &UnifiedBatch, items: &[UnifiedItem]) -> Value {
        let terminal = batch.status.is_terminal();
        let results = terminal.then(|| {
            items
                .iter()
                .map(|i| render_result(batch, i))
                .collect::<Vec<_>>()
        });
        serde_json::json!({
            "id": batch.batch_id,
            "object": "batch",
            "endpoint": batch.endpoint,
            "model": batch.model,
            "completion_window": "24h",
            "status": status_name(batch.status),
            "created_at": unix_secs(batch.created_at_ms),
            "finalized_at": batch.completed_at_ms.map(unix_secs),
            "expires_at": null,
            "provider": "sim-mock",
            "error": batch.batch_error.as_ref().map(|e| serde_json::json!({ "message": e.message })),
            "request_counts": {
                "total": batch.counts.total,
                "completed": batch.counts.completed,
                "failed": batch.counts.failed,
            },
            "usage": {
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
            },
            // `output` omitted: only file/BigQuery-input batches report one.
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
        let event = match batch.status {
            BatchStatus::Completed => "batch.completed",
            BatchStatus::Failed => "batch.failed",
            BatchStatus::Cancelled => "batch.cancelled",
            _ => "batch.updated",
        };
        serde_json::json!({
            "id": format!("wh_{}", batch.batch_id),
            "event": event,
            "created": unix_secs(batch.updated_at_ms),
            "data": self.render_status(batch, items),
        })
    }

    fn render_error(&self, err: &SimError) -> Value {
        serde_json::json!({
            "error": { "code": err.http_status, "message": err.message }
        })
    }

    fn create_http_status(&self) -> u16 {
        202
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{ChaosConfig, EchoData, ProviderKind, RequestCounts, WebhookStatus};
    use serde_json::json;

    fn batch(status: BatchStatus) -> UnifiedBatch {
        UnifiedBatch {
            batch_id: "batch_or1".to_string(),
            provider: ProviderKind::OpenRouter,
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
            model: Some("openai/gpt-4o".to_string()),
            endpoint: Some("/v1/chat/completions".to_string()),
            poll_count: 0,
            inline_items: vec![],
        }
    }

    #[test]
    fn create_body_validation() {
        let a = OpenRouterAdapter;
        // missing endpoint/model/requests
        assert!(a.ingest(IngestRequest::CreateBody(&json!({}))).is_err());
        // bad endpoint
        assert!(
            a.ingest(IngestRequest::CreateBody(
                &json!({"endpoint": "/v1/nope", "model": "m", "requests": []})
            ))
            .is_err()
        );
        // empty requests
        let err = a
            .ingest(IngestRequest::CreateBody(
                &json!({"endpoint": "/v1/chat/completions", "model": "m", "requests": []}),
            ))
            .unwrap_err();
        assert_eq!(err.http_status, 400);
        // unit missing custom_id
        assert!(
            a.ingest(IngestRequest::CreateBody(&json!({
                "endpoint": "/v1/chat/completions", "model": "m",
                "requests": [{"body": {}}],
            })))
            .is_err()
        );
        // non-https callback
        assert!(
            a.ingest(IngestRequest::CreateBody(&json!({
                "endpoint": "/v1/chat/completions", "model": "m",
                "callback_url": "http://x.test",
                "requests": [{"custom_id": "a", "body": {"messages": []}}],
            })))
            .is_err()
        );
        // happy path incl. native callback
        match a
            .ingest(IngestRequest::CreateBody(&json!({
                "endpoint": "/v1/responses", "model": "m",
                "callback_url": "https://cb.test/h",
                "requests": [{"custom_id": "a", "body": {"input": []}}],
            })))
            .unwrap()
        {
            IngestOutcome::CreateIntent(intent) => {
                assert!(matches!(intent.source, ItemSource::Inline(_)));
                assert_eq!(intent.callback_url.as_deref(), Some("https://cb.test/h"));
                assert_eq!(intent.endpoint.as_deref(), Some("/v1/responses"));
            }
            IngestOutcome::Item(_) => panic!("expected intent"),
        }
    }

    #[test]
    fn status_embeds_results_when_terminal() {
        let a = OpenRouterAdapter;
        let item = UnifiedItem {
            custom_id: "a".to_string(),
            endpoint: "/v1/chat/completions".to_string(),
            body_style: Some(crate::domain::BodyStyle::ChatCompletions),
            input_payload: json!({}),
            outcome: Some(UnifiedOutcome::Success(EchoData {
                input: json!({}),
                truncated: false,
            })),
        };
        let pending = a.render_status(&batch(BatchStatus::InProgress), &[item.clone()]);
        assert_eq!(pending["results"], json!(null));
        assert_eq!(pending["status"], json!("in_progress"));
        let done = a.render_status(&batch(BatchStatus::Completed), &[item]);
        assert_eq!(done["results"][0]["response"]["body"]["object"], json!("chat.completion"));
        assert_eq!(done["object"], json!("batch"));
        assert!(done.get("output").is_none());
    }
}
