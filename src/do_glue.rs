//! Worker I/O glue: Durable Object structs, internal stub RPC, and the
//! edge request handler. All decisions live in the stores/adapters; this
//! module only moves bytes and sets alarms.
//!
//! File batches: the coordinator `fetch` handler pages the registry (async),
//! ingests lines via the adapter, then submits inline — the store's sync
//! [`FileSource`] copy path stays for logic reuse and unit tests.

use serde::Serialize;
use serde_json::Value;
use worker::{
    DurableObject, Env, Headers, Method, Request, RequestInit, Response, State, Stub,
    durable_object,
};

use crate::adapters::adapter_for;
use crate::adapters::common::{IngestOutcome, IngestRequest, ItemSource, RenderedResults};
use crate::config::{
    COPY_PAGE_ROWS, HDR_SEED_USED, REGISTRY_CLEANUP_INTERVAL_MS, REGISTRY_DO_NAME,
    RETRY_AFTER_SECS, WEBHOOK_MAX_ITEMS, new_batch_id, new_file_id, parse_embedded_file_id,
};
use crate::control::{EnvDefaults, check_auth, parse_create_controls};
use crate::coordinator::{
    AlarmOutcome, BatchPort, CoordRequest, CoordResponse, CoordinatorStore, NoFileSource,
};
use crate::db::{Db, WorkerDb, col_text};
use crate::domain::{
    BatchStatus, ProviderKind, RequestCounts, SimError, UnifiedBatch, UnifiedItem,
    WebhookStatus,
};
use crate::registry::{FilePage, RegistryRequest, RegistryResponse, RegistryStore};
use crate::router::{Route, match_route, parse_openai_upload, split_jsonl};

// ---------------------------------------------------------------------------
// stubs + RPC
// ---------------------------------------------------------------------------

fn registry_stub(env: &Env) -> Result<Stub, worker::Error> {
    Ok(env
        .durable_object("FILE_REGISTRY")?
        .id_from_name(REGISTRY_DO_NAME)?
        .get_stub()?)
}

fn coord_stub(env: &Env, batch_id: &str) -> Result<Stub, worker::Error> {
    Ok(env
        .durable_object("BATCH_COORDINATOR")?
        .id_from_name(batch_id)?
        .get_stub()?)
}

async fn do_rpc(stub: &Stub, body: &impl Serialize) -> Result<(u16, Value), worker::Error> {
    let payload = serde_json::to_string(body).unwrap_or_default();
    let mut init = RequestInit::new();
    init.with_method(Method::Post)
        .with_body(Some(worker::wasm_bindgen::JsValue::from_str(&payload)));
    let req = Request::new_with_init("https://do.internal/rpc", &init)?;
    let mut resp = stub.fetch_with_request(req).await?;
    let status = resp.status_code();
    let text = resp.text().await.unwrap_or_default();
    let value: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
    Ok((status, value))
}

fn rpc_sim_error(status: u16, value: &Value) -> SimError {
    serde_json::from_value::<SimError>(value.clone())
        .unwrap_or_else(|_| SimError::internal_error(format!("do rpc failed: {status}")))
}

fn now_ms() -> u64 {
    worker::Date::now().as_millis()
}

fn storage_err(e: String) -> SimError {
    SimError::internal_error(e)
}

// ---------------------------------------------------------------------------
// FileRegistry DO
// ---------------------------------------------------------------------------

#[durable_object]
pub struct FileRegistry {
    state: State,
    #[allow(dead_code)]
    env: Env,
}

impl DurableObject for FileRegistry {
    fn new(state: State, env: Env) -> Self {
        Self { state, env }
    }

    async fn fetch(&self, mut req: Request) -> Result<Response, worker::Error> {
        // Native text parse: req.json() routes through f64 and mangles u64s.
        let text = req.text().await.unwrap_or_default();
        let op: RegistryRequest = serde_json::from_str(&text)
            .map_err(|e| worker::Error::RustError(format!("bad registry rpc: {e}")))?;
        let db = WorkerDb::new(self.state.storage().sql());
        let store = RegistryStore::new(&db);
        store.init().map_err(worker::Error::RustError)?;
        match registry_op(&store, &self.state, op).await {
            Ok(v) => Response::from_json(&v),
            Err(e) => {
                let status = e.http_status;
                Ok(Response::from_json(&e)?.with_status(status))
            }
        }
    }

    async fn alarm(&self) -> Result<Response, worker::Error> {
        let db = WorkerDb::new(self.state.storage().sql());
        let store = RegistryStore::new(&db);
        store.init().map_err(worker::Error::RustError)?;
        let now = now_ms() as i64;
        let pruned = store.prune_expired(now).unwrap_or(0);
        self.state
            .storage()
            .set_alarm(REGISTRY_CLEANUP_INTERVAL_MS)
            .await?;
        Response::ok(format!("pruned {pruned}"))
    }
}

async fn registry_op(
    store: &RegistryStore<'_, WorkerDb>,
    state: &State,
    op: RegistryRequest,
) -> Result<RegistryResponse, SimError> {
    match op {
        RegistryRequest::Put { file_id, provider, lines, now_ms } => {
            store.put(&file_id, provider, &lines, now_ms).map_err(storage_err)?;
            // Bootstrap the hourly prune sweep (idempotent overwrite).
            if state.storage().get_alarm().await.unwrap_or(Some(0)).is_none() {
                state
                    .storage()
                    .set_alarm(REGISTRY_CLEANUP_INTERVAL_MS)
                    .await
                    .map_err(|e| SimError::internal_error(e.to_string()))?;
            }
            Ok(RegistryResponse::ack())
        }
        RegistryRequest::MarkReferenced { file_id, now_ms } => {
            let found = store.mark_referenced(&file_id, now_ms).map_err(storage_err)?;
            Ok(RegistryResponse::found(found))
        }
        RegistryRequest::GetPage { file_id, offset, limit } => {
            match store.get_page(&file_id, offset, limit).map_err(storage_err)? {
                Some(page) => Ok(RegistryResponse::page(page)),
                None => Ok(RegistryResponse::page(FilePage {
                    found: false,
                    total: 0,
                    lines: Vec::new(),
                })),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// BatchCoordinator DO
// ---------------------------------------------------------------------------

#[durable_object]
pub struct BatchCoordinator {
    state: State,
    env: Env,
}

/// Page a registry file into ingested items (async transport for the sync
/// store path). Empty/oversize checks mirror the store's (early 400 before
/// wasting ingest work); dup/line checks stay in the store.
async fn copy_file_items(
    env: &Env,
    provider: ProviderKind,
    file_id: &str,
) -> Result<Vec<UnifiedItem>, SimError> {
    use crate::config::MAX_ITEMS_PER_BATCH;
    let stub =
        registry_stub(env).map_err(|e| SimError::internal_error(e.to_string()))?;
    let adapter = adapter_for(provider);
    let fetch_page = async |offset: i64| -> Result<FilePage, SimError> {
        let (status, v) = do_rpc(
            &stub,
            &RegistryRequest::GetPage {
                file_id: file_id.to_string(),
                offset,
                limit: COPY_PAGE_ROWS,
            },
        )
        .await
        .map_err(|e| SimError::internal_error(e.to_string()))?;
        if status != 200 {
            return Err(rpc_sim_error(status, &v));
        }
        serde_json::from_value::<RegistryResponse>(v)
            .map_err(|e| SimError::internal_error(e.to_string()))?
            .page
            .ok_or_else(|| SimError::internal_error("registry page missing"))
    };
    let first = fetch_page(0).await?;
    if !first.found {
        return Err(SimError::not_found(&format!("file '{file_id}'")));
    }
    if first.total == 0 {
        return Err(SimError::bad_request("input file contains no requests"));
    }
    if first.total > MAX_ITEMS_PER_BATCH as i64 {
        return Err(SimError::bad_request(format!(
            "input file holds {} requests, exceeding the {MAX_ITEMS_PER_BATCH} limit",
            first.total
        )));
    }
    let mut items = Vec::with_capacity(first.total as usize);
    let mut offset = 0i64;
    loop {
        let page = if offset == 0 { first.clone() } else { fetch_page(offset).await? };
        for (i, line) in page.lines.iter().enumerate() {
            let number = (offset as usize) + i + 1;
            match adapter.ingest(IngestRequest::FileLine { line, number })? {
                IngestOutcome::Item(item) => items.push(item),
                IngestOutcome::CreateIntent(_) => {
                    return Err(SimError::internal_error(format!(
                        "line {number}: unexpected create intent"
                    )));
                }
            }
        }
        offset += page.lines.len() as i64;
        if offset >= page.total || page.lines.is_empty() {
            break;
        }
    }
    let (status, v) = do_rpc(
        &stub,
        &RegistryRequest::MarkReferenced {
            file_id: file_id.to_string(),
            now_ms: now_ms() as i64,
        },
    )
    .await
    .map_err(|e| SimError::internal_error(e.to_string()))?;
    if status != 200 {
        return Err(rpc_sim_error(status, &v));
    }
    Ok(items)
}

fn terminal_event_name(s: BatchStatus) -> &'static str {
    match s {
        BatchStatus::Completed => "completed",
        BatchStatus::Failed => "failed",
        BatchStatus::Cancelled => "cancelled",
        _ => "updated",
    }
}

impl DurableObject for BatchCoordinator {
    fn new(state: State, env: Env) -> Self {
        Self { state, env }
    }

    async fn fetch(&self, mut req: Request) -> Result<Response, worker::Error> {
        let text = req.text().await.unwrap_or_default();
        let op: CoordRequest = serde_json::from_str(&text)
            .map_err(|e| worker::Error::RustError(format!("bad coord rpc: {e}")))?;
        let db = WorkerDb::new(self.state.storage().sql());
        let store = CoordinatorStore::new(&db);
        store.init().map_err(worker::Error::RustError)?;
        match coord_op(&store, &self.state, &self.env, op, now_ms()).await {
            Ok(v) => Response::from_json(&v),
            Err(e) => {
                let status = e.http_status;
                Ok(Response::from_json(&e)?.with_status(status))
            }
        }
    }

    async fn alarm(&self) -> Result<Response, worker::Error> {
        let db = WorkerDb::new(self.state.storage().sql());
        let store = CoordinatorStore::new(&db);
        store.init().map_err(worker::Error::RustError)?;
        let now = now_ms();
        let own: Vec<Vec<crate::db::DbValue>> =
            db.exec("SELECT id FROM batches LIMIT 1", &[]).unwrap_or_default();
        let Some(row) = own.first() else {
            return Response::ok("empty");
        };
        let batch_id = col_text(row, 0);
        match store.alarm_tick(&batch_id, now) {
            Err(e) => Response::ok(format!("tick error: {}", e.message)),
            Ok(AlarmOutcome::Noop) => Response::ok("noop"),
            Ok(AlarmOutcome::Advanced { alarm_ms }) => {
                // set_alarm takes an offset, not an absolute time.
                let delay = alarm_ms.saturating_sub(now) as i64;
                self.state.storage().set_alarm(delay).await?;
                Response::ok("advanced")
            }
            Ok(AlarmOutcome::Terminal { dispatch_webhook }) => {
                if dispatch_webhook {
                    dispatch_terminal_webhook(&store, &batch_id).await;
                }
                Response::ok("terminal")
            }
        }
    }
}

async fn coord_op(
    store: &CoordinatorStore<'_, WorkerDb>,
    state: &State,
    env: &Env,
    op: CoordRequest,
    now: u64,
) -> Result<CoordResponse, SimError> {
    match op {
        CoordRequest::Create { mut batch } => {
            if batch.input_file_id.is_some() && batch.inline_items.is_empty() {
                let fid = batch.input_file_id.clone().unwrap_or_default();
                batch.inline_items = copy_file_items(env, batch.provider, &fid).await?;
            }
            let submit = store.submit(batch, &NoFileSource, now)?;
            state
                .storage()
                .set_alarm(submit.alarm_ms.saturating_sub(now) as i64)
                .await
                .map_err(|e| SimError::internal_error(e.to_string()))?;
            Ok(CoordResponse {
                batch: Some(submit.batch),
                alarm_ms: Some(submit.alarm_ms),
                ..Default::default()
            })
        }
        CoordRequest::Poll { batch_id } => {
            let o = store.poll(&batch_id, now)?;
            Ok(CoordResponse {
                batch: Some(o.batch),
                items: Some(o.items),
                edge_status: o.edge_status,
                ..Default::default()
            })
        }
        CoordRequest::FetchItem { batch_id, custom_id } => {
            let (batch, item) = store.fetch_item_with_batch(&batch_id, &custom_id)?;
            Ok(CoordResponse {
                batch: Some(batch),
                item: Some(item),
                ..Default::default()
            })
        }
        CoordRequest::FetchAll { batch_id } => {
            let (batch, items) = store.fetch_all(&batch_id)?;
            Ok(CoordResponse {
                batch: Some(batch),
                items: Some(items),
                ..Default::default()
            })
        }
        CoordRequest::Cancel { batch_id } => {
            let batch = store.cancel(&batch_id, now)?;
            state
                .storage()
                .delete_alarm()
                .await
                .map_err(|e| SimError::internal_error(e.to_string()))?;
            Ok(CoordResponse {
                batch: Some(batch),
                ..Default::default()
            })
        }
        CoordRequest::SetWebhookStatus { batch_id, status } => {
            store.set_webhook_status(&batch_id, status)?;
            Ok(CoordResponse::default())
        }
    }
}

async fn dispatch_terminal_webhook<D: Db>(store: &CoordinatorStore<'_, D>, batch_id: &str) {
    let Ok((batch, items)) = store.fetch_all(batch_id) else {
        return;
    };
    let Some(url) = batch.callback_url.clone() else {
        return;
    };
    let end = items.len().min(WEBHOOK_MAX_ITEMS);
    let payload = adapter_for(batch.provider).build_webhook(&batch, &items[..end]);
    let dedupe = crate::webhook::dedupe_value(&batch.batch_id, terminal_event_name(batch.status));
    let ok = crate::webhook::dispatch(&url, batch.webhook_secret.as_deref(), &dedupe, &payload).await;
    let _ = store.set_webhook_status(
        batch_id,
        if ok { WebhookStatus::Delivered } else { WebhookStatus::Failed },
    );
}

// ---------------------------------------------------------------------------
// edge responses
// ---------------------------------------------------------------------------

fn json_response(
    value: &Value,
    status: u16,
    extra: &[(&str, &str)],
) -> Result<Response, worker::Error> {
    let headers = Headers::new();
    headers.set("content-type", "application/json")?;
    for (k, v) in extra {
        headers.set(k, v)?;
    }
    Ok(Response::from_json(value)?.with_headers(headers).with_status(status))
}

fn text_response(
    body: String,
    content_type: &str,
    status: u16,
) -> Result<Response, worker::Error> {
    let headers = Headers::new();
    headers.set("content-type", content_type)?;
    Ok(Response::ok(body)?.with_headers(headers).with_status(status))
}

fn err_response(
    provider: Option<ProviderKind>,
    err: &SimError,
) -> Result<Response, worker::Error> {
    let (value, status) = match provider {
        Some(p) => {
            let a = adapter_for(p);
            (a.render_error(err), a.error_http_status(err))
        }
        None => (
            serde_json::json!({"error": {"message": err.message, "code": err.code}}),
            err.http_status,
        ),
    };
    json_response(&value, status, &[])
}

fn infra_response(provider: Option<ProviderKind>, e: worker::Error) -> Result<Response, worker::Error> {
    err_response(provider, &SimError::internal_error(e.to_string()))
}

// ---------------------------------------------------------------------------
// edge handler
// ---------------------------------------------------------------------------

pub async fn handle_edge(mut req: Request, env: Env) -> Result<Response, worker::Error> {
    let method = match req.method() {
        Method::Get => "GET",
        Method::Post => "POST",
        _ => "OTHER",
    };
    let path = req.path();
    let route = match_route(method, &path);
    let provider = route.provider();
    let defaults = EnvDefaults::from_getter(&|k| env.var(k).ok().map(|v| v.to_string()));
    let auth = req.headers().get("authorization").ok().flatten();
    if let Some(err) = check_auth(auth.as_deref(), &defaults) {
        return err_response(provider, &err);
    }
    match route {
        Route::NotFound => err_response(None, &SimError::not_found("route")),
        Route::OpenAiCreate => handle_create(&mut req, &env, ProviderKind::OpenAI, &defaults, None).await,
        Route::OpenRouterCreate => {
            handle_create(&mut req, &env, ProviderKind::OpenRouter, &defaults, None).await
        }
        Route::SailCreate => handle_create(&mut req, &env, ProviderKind::Sail, &defaults, None).await,
        Route::GeminiCreate { model } => {
            handle_create(&mut req, &env, ProviderKind::Gemini, &defaults, Some(model)).await
        }
        Route::OpenAiPoll { batch_id } => handle_poll(&env, ProviderKind::OpenAI, &batch_id).await,
        Route::OpenRouterPoll { batch_id } => {
            handle_poll(&env, ProviderKind::OpenRouter, &batch_id).await
        }
        Route::SailPoll { batch_id } => handle_poll(&env, ProviderKind::Sail, &batch_id).await,
        Route::GeminiPoll { batch_id } => handle_poll(&env, ProviderKind::Gemini, &batch_id).await,
        Route::OpenAiCancel { batch_id } => {
            handle_cancel(&env, ProviderKind::OpenAI, &batch_id).await
        }
        Route::OpenRouterCancel { batch_id } => {
            handle_cancel(&env, ProviderKind::OpenRouter, &batch_id).await
        }
        Route::SailCancel { batch_id } => handle_cancel(&env, ProviderKind::Sail, &batch_id).await,
        Route::GeminiCancel { batch_id } => {
            handle_cancel(&env, ProviderKind::Gemini, &batch_id).await
        }
        Route::SailItem { batch_id, custom_id } => {
            handle_sail_item(&env, &batch_id, &custom_id).await
        }
        Route::OpenAiUpload => handle_openai_upload(&mut req, &env).await,
        Route::GeminiUpload => handle_gemini_upload(&mut req, &env).await,
        Route::OpenAiFileContent { file_id } => handle_openai_content(&env, &file_id).await,
        Route::GeminiDownload { file_id } => handle_gemini_download(&env, &file_id).await,
    }
}

async fn coord_call(
    env: &Env,
    batch_id: &str,
    op: &CoordRequest,
) -> Result<CoordResponse, SimError> {
    let stub = coord_stub(env, batch_id).map_err(|e| SimError::internal_error(e.to_string()))?;
    let (status, v) = do_rpc(&stub, op)
        .await
        .map_err(|e| SimError::internal_error(e.to_string()))?;
    if status != 200 {
        return Err(rpc_sim_error(status, &v));
    }
    serde_json::from_value::<CoordResponse>(v)
        .map_err(|e| SimError::internal_error(e.to_string()))
}

async fn handle_create(
    req: &mut Request,
    env: &Env,
    provider: ProviderKind,
    defaults: &EnvDefaults,
    url_model: Option<String>,
) -> Result<Response, worker::Error> {
    let text = req.text().await.unwrap_or_default();
    let body: Value = match serde_json::from_str(&text) {
        Ok(v) => v,
        Err(e) => {
            return err_response(
                Some(provider),
                &SimError::bad_request(format!("invalid JSON body: {e}")),
            );
        }
    };
    let adapter = adapter_for(provider);
    let intent = match adapter.ingest(IngestRequest::CreateBody(&body)) {
        Ok(IngestOutcome::CreateIntent(i)) => i,
        Ok(IngestOutcome::Item(_)) => {
            return err_response(
                Some(provider),
                &SimError::bad_request("unexpected item from create body"),
            );
        }
        Err(e) => return err_response(Some(provider), &e),
    };
    let get = |k: &str| req.headers().get(k).ok().flatten();
    let controls = parse_create_controls(&get, defaults, fastrand::u64(..));
    // Explicit test header wins over the native body field.
    let callback_url = controls.callback_url.or(intent.callback_url);
    let webhook_secret = if callback_url.is_some() {
        controls.webhook_secret
    } else {
        None
    };
    let (input_file_id, inline_items) = match intent.source {
        ItemSource::Inline(items) => (None, items),
        ItemSource::FileRef(fid) => (Some(fid), Vec::new()),
    };
    let batch = UnifiedBatch {
        batch_id: new_batch_id(),
        provider,
        status: BatchStatus::Validating,
        created_at_ms: 0,
        updated_at_ms: 0,
        completed_at_ms: None,
        chaos: controls.chaos.clone(),
        callback_url: callback_url.clone(),
        webhook_secret,
        webhook_status: if callback_url.is_some() {
            WebhookStatus::Pending
        } else {
            WebhookStatus::Skipped
        },
        input_file_id,
        counts: RequestCounts::default(),
        batch_error: None,
        model: intent.model.or(url_model),
        endpoint: intent.endpoint,
        poll_count: 0,
        inline_items,
    };
    let batch_id = batch.batch_id.clone();
    let created = match coord_call(env, &batch_id, &CoordRequest::Create { batch }).await {
        Ok(r) => r,
        Err(e) => return err_response(Some(provider), &e),
    };
    let stored = created.batch.unwrap_or_else(|| UnifiedBatch {
        batch_id: batch_id.clone(),
        provider,
        status: BatchStatus::Validating,
        created_at_ms: now_ms(),
        updated_at_ms: now_ms(),
        completed_at_ms: None,
        chaos: controls.chaos.clone(),
        callback_url,
        webhook_secret: None,
        webhook_status: WebhookStatus::Skipped,
        input_file_id: None,
        counts: RequestCounts::default(),
        batch_error: None,
        model: None,
        endpoint: None,
        poll_count: 0,
        inline_items: Vec::new(),
    });
    let status_doc = adapter.render_status(&stored, &[]);
    json_response(
        &status_doc,
        adapter.create_http_status(),
        &[(HDR_SEED_USED, &controls.chaos.seed.to_string())],
    )
}

async fn handle_poll(
    env: &Env,
    provider: ProviderKind,
    batch_id: &str,
) -> Result<Response, worker::Error> {
    let out = match coord_call(env, batch_id, &CoordRequest::Poll {
        batch_id: batch_id.to_string(),
    })
    .await
    {
        Ok(r) => r,
        Err(e) => return err_response(Some(provider), &e),
    };
    if let Some(chaos_status) = out.edge_status {
        let err = SimError {
            http_status: chaos_status,
            code: if chaos_status == 429 { "rate_limit".to_string() } else { "unavailable".to_string() },
            message: "simulated transient edge congestion".to_string(),
        };
        let value = adapter_for(provider).render_error(&err);
        if chaos_status == 429 {
            return json_response(&value, 429, &[("retry-after", &RETRY_AFTER_SECS.to_string())]);
        }
        return json_response(&value, chaos_status, &[]);
    }
    let (batch, items) = match (out.batch, out.items) {
        (Some(b), Some(i)) => (b, i),
        _ => return err_response(Some(provider), &SimError::internal_error("poll shape")),
    };
    let doc = adapter_for(provider).render_status(&batch, &items);
    json_response(&doc, 200, &[])
}

async fn handle_cancel(
    env: &Env,
    provider: ProviderKind,
    batch_id: &str,
) -> Result<Response, worker::Error> {
    let out = match coord_call(env, batch_id, &CoordRequest::Cancel {
        batch_id: batch_id.to_string(),
    })
    .await
    {
        Ok(r) => r,
        Err(e) => return err_response(Some(provider), &e),
    };
    // Gemini's real cancel returns an empty object.
    if provider == ProviderKind::Gemini {
        return json_response(&serde_json::json!({}), 200, &[]);
    }
    match out.batch {
        Some(b) => {
            let doc = adapter_for(provider).render_status(&b, &[]);
            json_response(&doc, 200, &[])
        }
        None => err_response(Some(provider), &SimError::internal_error("cancel shape")),
    }
}

async fn handle_sail_item(
    env: &Env,
    batch_id: &str,
    custom_id: &str,
) -> Result<Response, worker::Error> {
    let out = match coord_call(env, batch_id, &CoordRequest::FetchItem {
        batch_id: batch_id.to_string(),
        custom_id: custom_id.to_string(),
    })
    .await
    {
        Ok(r) => r,
        Err(e) => return err_response(Some(ProviderKind::Sail), &e),
    };
    match (out.batch, out.item) {
        (Some(b), Some(item)) if b.status.is_terminal() => {
            let doc = adapter_for(ProviderKind::Sail).render_item(&b, &item);
            json_response(&doc, 200, &[])
        }
        (Some(_), _) => err_response(
            Some(ProviderKind::Sail),
            &SimError::not_found(&format!("results for item '{custom_id}' not ready")),
        ),
        _ => err_response(Some(ProviderKind::Sail), &SimError::internal_error("item shape")),
    }
}

async fn handle_openai_upload(
    req: &mut Request,
    env: &Env,
) -> Result<Response, worker::Error> {
    let body = req.bytes().await.unwrap_or_default();
    let ct = req.headers().get("content-type").ok().flatten();
    let upload = match parse_openai_upload(ct.as_deref(), &body)
        .and_then(|u| split_jsonl(&u.bytes).map(|lines| (u.filename, u.bytes.len(), lines)))
    {
        Ok(v) => v,
        Err(e) => return err_response(Some(ProviderKind::OpenAI), &e),
    };
    let (filename, byte_len, lines) = upload;
    let file_id = new_file_id();
    let stub = match registry_stub(env) {
        Ok(s) => s,
        Err(e) => return infra_response(Some(ProviderKind::OpenAI), e),
    };
    let (status, v) = match do_rpc(&stub, &RegistryRequest::Put {
        file_id: file_id.clone(),
        provider: ProviderKind::OpenAI,
        lines,
        now_ms: now_ms() as i64,
    })
    .await
    {
        Ok(r) => r,
        Err(e) => return infra_response(Some(ProviderKind::OpenAI), e),
    };
    if status != 200 {
        return err_response(Some(ProviderKind::OpenAI), &rpc_sim_error(status, &v));
    }
    json_response(
        &serde_json::json!({
            "id": file_id,
            "object": "file",
            "bytes": byte_len,
            "created_at": now_ms() / 1000,
            "filename": filename,
            "purpose": "batch",
        }),
        200,
        &[],
    )
}

async fn handle_gemini_upload(
    req: &mut Request,
    env: &Env,
) -> Result<Response, worker::Error> {
    let body = req.bytes().await.unwrap_or_default();
    let ct = req.headers().get("content-type").ok().flatten();
    let lines = match crate::adapters::gemini::extract_upload_payload(ct.as_deref(), &body)
        .and_then(|payload| split_jsonl(&payload))
    {
        Ok(l) => l,
        Err(e) => return err_response(Some(ProviderKind::Gemini), &e),
    };
    let file_id = new_file_id();
    let stub = match registry_stub(env) {
        Ok(s) => s,
        Err(e) => return infra_response(Some(ProviderKind::Gemini), e),
    };
    let (status, v) = match do_rpc(&stub, &RegistryRequest::Put {
        file_id: file_id.clone(),
        provider: ProviderKind::Gemini,
        lines,
        now_ms: now_ms() as i64,
    })
    .await
    {
        Ok(r) => r,
        Err(e) => return infra_response(Some(ProviderKind::Gemini), e),
    };
    if status != 200 {
        return err_response(Some(ProviderKind::Gemini), &rpc_sim_error(status, &v));
    }
    json_response(
        &crate::adapters::gemini::render_file_object(&file_id, body.len(), now_ms()),
        200,
        &[],
    )
}

async fn handle_openai_content(env: &Env, file_id: &str) -> Result<Response, worker::Error> {
    if let Some((batch_id, is_err)) = parse_embedded_file_id(file_id) {
        let out = match coord_call(env, batch_id, &CoordRequest::FetchAll {
            batch_id: batch_id.to_string(),
        })
        .await
        {
            Ok(r) => r,
            Err(e) => return err_response(Some(ProviderKind::OpenAI), &e),
        };
        let (batch, items) = match (out.batch, out.items) {
            (Some(b), Some(i)) => (b, i),
            _ => return err_response(Some(ProviderKind::OpenAI), &SimError::internal_error("content shape")),
        };
        if batch.status != BatchStatus::Completed {
            return err_response(
                Some(ProviderKind::OpenAI),
                &SimError::not_found("output not ready"),
            );
        }
        let files = adapter_for(ProviderKind::OpenAI).render_results(&batch, &items);
        let doc = if is_err {
            match files.secondary {
                Some(d) => d,
                None => {
                    return err_response(
                        Some(ProviderKind::OpenAI),
                        &SimError::not_found("no error output for this batch"),
                    );
                }
            }
        } else {
            files.primary
        };
        return match doc {
            RenderedResults::Jsonl(text) => text_response(text, "application/jsonl", 200),
            RenderedResults::Json(v) => json_response(&v, 200, &[]),
        };
    }
    serve_registry_file(env, ProviderKind::OpenAI, file_id, "application/jsonl").await
}

async fn handle_gemini_download(env: &Env, file_id: &str) -> Result<Response, worker::Error> {
    if let Some((batch_id, is_err)) = parse_embedded_file_id(file_id) {
        if is_err {
            // Gemini mixes errors into the single responses file.
            return err_response(
                Some(ProviderKind::Gemini),
                &SimError::not_found(&format!("file '{file_id}'")),
            );
        }
        let out = match coord_call(env, batch_id, &CoordRequest::FetchAll {
            batch_id: batch_id.to_string(),
        })
        .await
        {
            Ok(r) => r,
            Err(e) => return err_response(Some(ProviderKind::Gemini), &e),
        };
        let (batch, items) = match (out.batch, out.items) {
            (Some(b), Some(i)) => (b, i),
            _ => {
                return err_response(
                    Some(ProviderKind::Gemini),
                    &SimError::internal_error("download shape"),
                );
            }
        };
        if batch.status != BatchStatus::Completed {
            return err_response(
                Some(ProviderKind::Gemini),
                &SimError::not_found("output not ready"),
            );
        }
        let files = adapter_for(ProviderKind::Gemini).render_results(&batch, &items);
        return match files.primary {
            RenderedResults::Jsonl(text) => text_response(text, "application/octet-stream", 200),
            RenderedResults::Json(v) => json_response(&v, 200, &[]),
        };
    }
    serve_registry_file(env, ProviderKind::Gemini, file_id, "application/octet-stream").await
}

async fn serve_registry_file(
    env: &Env,
    provider: ProviderKind,
    file_id: &str,
    content_type: &str,
) -> Result<Response, worker::Error> {
    let stub = match registry_stub(env) {
        Ok(s) => s,
        Err(e) => return infra_response(Some(provider), e),
    };
    let mut offset = 0i64;
    let mut all = Vec::new();
    loop {
        let (status, v) = match do_rpc(&stub, &RegistryRequest::GetPage {
            file_id: file_id.to_string(),
            offset,
            limit: COPY_PAGE_ROWS,
        })
        .await
        {
            Ok(r) => r,
            Err(e) => return infra_response(Some(provider), e),
        };
        if status != 200 {
            return err_response(Some(provider), &rpc_sim_error(status, &v));
        }
        let page = match serde_json::from_value::<RegistryResponse>(v).ok().and_then(|r| r.page) {
            Some(p) => p,
            None => {
                return err_response(Some(provider), &SimError::internal_error("registry shape"));
            }
        };
        if !page.found {
            return err_response(
                Some(provider),
                &SimError::not_found(&format!("file '{file_id}'")),
            );
        }
        offset += page.lines.len() as i64;
        all.extend(page.lines);
        if offset >= page.total || all.len() as i64 >= page.total {
            break;
        }
    }
    text_response(all.join("\n"), content_type, 200)
}
