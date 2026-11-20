# Multi-Provider LLM Batch API Simulator — Execution Plan

## Context

Greenfield Rust (`workers-rs`, `wasm32-unknown-unknown`) Cloudflare Worker simulating async LLM batch APIs for OpenAI, OpenRouter, Sail Research, and Google Gemini behind one deployment. Grilled and locked 12 decision branches against `local://paste-1.md` (empty repo, no code to reuse). End state: `cargo test` contract suites per provider plus a `wrangler dev`-verified worker where upload → create → poll → fetch-content → cancel → webhook flows work per provider with seeded, reproducible chaos.

## Approach

Build order: scaffold → domain core → storage DOs → lifecycle → adapters → router → verification. Each step leaves `cargo test` green.

### Step 1 — Scaffold crate, bindings, control-plane contract

- `cargo init --lib`, `workers-rs` + `worker-macros`, `serde/serde_json`, `fastrand`, `hmac`, `sha2` only (no OpenSSL/async runtimes; wasm budget). Exact `workers-rs` version: latest stable at scaffold time (unverified — confirm first via crates.io).
- `wrangler.toml`: two DO classes `FileRegistry`, `BatchCoordinator`; `[vars]` defaults `SIM_VALIDATION_DELAY_MS=30000`, `SIM_PROCESSING_DELAY_MS=300000`, `SIM_JITTER_PCT=50`, `SIM_BATCH_FAIL_RATE=0.0`, `SIM_ITEM_FAIL_RATE=0.0`, `SIM_EDGE_CHAOS_RATE=0.0`, `SIM_REQUIRE_AUTH=0`.
- Control headers (exact literals, parsed once at edge, stored on batch): `X-Sim-Delay-Ms` (overrides both delays; `0` fast-forwards — required escape hatch from realistic defaults), `X-Sim-Fail-Rate`, `X-Sim-Item-Fail-Rate`, `X-Sim-Edge-Chaos-Rate`, `X-Sim-Seed` (u64; absent → random, echoed back in `X-Sim-Seed-Used` response header for replay), `X-Sim-Callback-Url`, `X-Sim-Webhook-Secret`. Unknown `X-Sim-*` ignored.
- Quotas (exact): 10k items/batch, 32MB upload, 1MB/JSONL line → synchronous provider-shaped `400` on exceed. No other quota surface.

### Step 2 — Provider-agnostic domain core (`src/domain.rs`, `src/chaos.rs`)

- Types: `ProviderKind { OpenAI, OpenRouter, Sail, Gemini }` (opaque routing tag; `match` exhaustively at adapter boundary, never in core), `BatchStatus { Validating, InProgress, Completed, Failed, Cancelled }`, `UnifiedBatch { batch_id, provider, status, created_at_ms, chaos: ChaosConfig, callback_url, webhook_secret: Option<String>, output_counts }`, `UnifiedItem { custom_id, endpoint, input_payload: Value, body_style: BodyStyle, outcome: Option<UnifiedOutcome> }`, `UnifiedOutcome { Success(Value) | Error(SimError) }`.
- `BodyStyle { ChatCompletions, Responses, GeminiNative }` — per-item sniff precedence (locked tangent): explicit URL/endpoint match first → payload shape (`messages[]`→chat, `input[]`/`instructions`→responses, `contents[]`→gemini-native) → default `ChatCompletions`; unrecognizable → item-level error at completion, never batch failure.
- `ChaosEngine`: `fastrand::Rng::seed(seed)` per batch; methods `jitter(base_ms)`, `roll_bool(rate)`, `pick_item_error()` (seeded 50/50 `429`/`500`). No equivalent exists — new code.
- Ports: `BatchPort { submit, poll, fetch_item, cancel }`, `FilePort { put_jsonl, get_jsonl_page }`. No list port (cancel-only scope locked).

### Step 3 — `FileRegistry` singleton DO, input uploads only (`src/registry.rs`)

- Named instance `idFromName("global-file-registry")`; SQLite tables `files(file_id PK, provider, bytes BLOB or rows, created_at_ms, referenced BOOL, expires_at_ms)`.
- `POST` upload path stores rows; `referenced=1` set by batch creation; periodic alarm `DELETE FROM files WHERE expires_at_ms < now()` — 24h TTL unreferenced, 7d referenced. Sharding contingency: if load tests saturate singleton, swap name for `registry-{hash%16}` (5-line router change, schema untouched).
- Paged read API (chunked-copy guardrail): `get_page(file_id, offset, limit)` with 256-row pages so copy-at-create keeps isolate memory flat on multi-MB uploads.
- Shared registry for OpenAI `file-*` and Gemini `files/*` ids; provider tag column prevents cross-provider id collision.

### Step 4 — `BatchCoordinator` DO: create, alarms, cancel, embedded outputs (`src/coordinator.rs`)

- `POST /batches` synchronously: validate `file_id` (missing → provider-shaped `404`), copy rows page-wise into local `items` table, seed RNG, persist `UnifiedBatch`, `set_alarm(now + jitter(validation_delay))`. Copy-at-create keeps all alarms local — no cross-DO I/O in alarm handlers ever.
- Alarm #1 (`validating`→`in_progress`): guard current state (at-least-once re-entry), roll batch-fail → `failed` or schedule Alarm #2 (`+ jitter(processing_delay)`).
- Alarm #2 (terminal): materialize per-item outcomes via seeded item-fail rolls; deep-echo success bodies (see Step 5); update counts; embed output/error artifacts as local SQLite rows (no FileStore write); dispatch webhook per Step 6. Idempotent: no-op unless status still `in_progress`.
- Cancel: `cancel()` checks state first — `completed|failed|cancelled` → provider-shaped conflict (`409` OpenAI/OpenRouter/Sail, `400 FAILED_PRECONDITION` Gemini); else status=`cancelled` + `delete_alarm()` immediately so a racing alarm cannot overwrite. Uniform `POST .../cancel` route on all four providers (extension where vendor lacks one); no `cancelling` intermediate state.
- Output ids encode owner: `file-out-{batch_id}`, `file-err-{batch_id}`, Gemini `responsesFile` likewise; `GET .../content` routes directly to owning batch DO. Timestamps assigned in-DO (`Date.now()`); adapters render (unix secs vs RFC3339).

### Step 5 — Provider adapters (`src/adapters/*.rs` + shared `src/adapters/common.rs`)

- Trait `BatchProviderAdapter { ingest, render_status, render_results, render_item, build_webhook, render_error }`; adding a `ProviderKind` variant must break compilation until all six are implemented (exhaustive matches, no wildcard).
- Shared helpers only: `ItemSource { Inline | FileRef }`, `ResponseSink { InlineArray | JsonlStream }`; adapters supply pure JSON-shape closures (`body` vs `params` vs `contents`; `google.rpc.Status` vs OpenAI envelopes). Copy this helper pattern for new providers; never branch core on provider.
- Success bodies: schema-valid skeleton per body-style + deep echo of normalized input reflected in a provider-natural field, reflected payload truncated at 8KB with `"truncated": true` (caps row growth from 1MB lines). Fixed `model: "sim-mock-1"`, zero usage, content includes `custom_id`.
- Error items: seeded `429` (`rate_limit_exceeded`-shaped) / `500` envelopes per provider; batch-fail renders terminal `failed` with provider envelope and empty successes.
- Status mapping: internal names render verbatim for OpenAI-family; Gemini wraps in LRO envelope `{ done: bool, metadata: { "@type": ..., state }, response | error }` with `response.inlinedResponses` when serialized results ≤256KB else `responsesFile` id (threshold locked).
- Sail: `GET /batches/{id}/{custom_id}` per-item lookup from local `items` table (404 when pending/unknown).
- Each adapter owns `#[cfg(test)]` fixtures transcribed from vendor docs; round-trip `ingest→render` asserted offline.

### Step 6 — Edge router, auth, edge chaos, webhooks (`src/router.rs`, `src/webhook.rs`)

- Prefix routing `/openai/v1/*`, `/openrouter/api/v1/*`, `/sail/v1/*`, `/gemini/v1beta/*`; unknown prefix → `404` JSON (not HTML).
- Auth: accept any key or none by default; `SIM_REQUIRE_AUTH=1` enforces `SIM_API_KEYS` allowlist (comma-separated) with provider-shaped `401`s. Applies uniformly before routing.
- Edge chaos: per-batch `edge_chaos_rate` rolled on every `GET` poll; on hit return `429` with `Retry-After: 1` (70%) or `503` (30%), seeded separately from outcome rolls so polls don't consume outcome RNG.
- Webhooks: OpenRouter native `callback_url` body field + `X-Sim-Callback-Url` on all providers; Alarm #2 dispatches provider-shaped payload with headers `Dedupe-Key: {batch_id}:{status}`, `X-Webhook-Signature: sha256=<hmac>` only when secret set; 3 inline attempts with 500ms/2s backoff, then persist `webhook_status` (`delivered|failed`) on batch. No alarm reschedule for webhooks.

## Critical files & anchors

Planned paths (greenfield — all to-create):

- `src/domain.rs` — `UnifiedBatch/UnifiedItem/BodyStyle/SimError`; every adapter + DO depends on it.
- `src/coordinator.rs` — alarm handlers + `cancel()` race guard; at-least-once correctness lives here.
- `src/adapters/common.rs` — `ItemSource`/`ResponseSink` + echo-truncation helper; reuse choke point.
- `src/adapters/gemini.rs` — LRO envelope + 256KB inline/file switch; most intricate wire shape.
- `wrangler.toml` — DO bindings + `[vars]` chaos/auth defaults; misconfig breaks all routing.

## Verification

Prereqs: `cargo`, `wrangler`/`workerd` (miniflare) for local E2E; all commands from repo root.

- `cargo test` — adapter fixture round-trips (4 providers × ingest/render/error), chaos determinism (same seed → byte-identical outcomes across 2 runs), BodyStyle sniff precedence table.
- Local E2E (`wrangler dev`, seeded `X-Sim-Delay-Ms: 50`): per provider — create → poll to terminal → assert status envelope + counts → fetch content (OpenAI JSONL parse; Gemini both sides of 256KB threshold via item-count sizing; Sail per-item lookup; OpenRouter inline results).
- Chaos matrix: `X-Sim-Fail-Rate: 1.0` → terminal `failed` envelope; `X-Sim-Item-Fail-Rate: 1.0` → all items errored with mixed 429/500; `X-Sim-Edge-Chaos-Rate: 1.0` → polls 429 (`Retry-After` present) /503; seed replay: same seed twice → identical item outcomes.
- Webhook harness (throwaway local HTTP server): asserts HMAC signature + `Dedupe-Key`; first 2 POSTs 500 → exactly 3 attempts then `webhook_status=delivered`; absent secret → no signature header.
- Lifecycle: cancel mid-flight → `cancelled`, alarm silenced (no later flip); cancel-after-terminal → 409/400 per provider; quotas (11k items / 33MB / 2MB line) → `400`; `SIM_REQUIRE_AUTH=1` + bad key → `401`, unset → pass.

## Assumptions & contingencies

- Realistic defaults (30s/300s) chosen over fast; if suites prove too slow in practice, lower `[vars]` defaults — header override already guarantees a fast path, no code change needed.
- OpenRouter/Sail cancel + `X-Sim-Callback-Url` are simulator extensions (vendors lack/unclear native support); if strict wire-purity is later demanded, gate them behind `SIM_EXTENSIONS=1` (default on) — router-only change.
- Singleton registry assumed sufficient; if saturated, shard as in Step 3 without schema change.
- Deep-echo truncation at 8KB assumed acceptable to clients; if a consumer needs full reflection, raise cap via `SIM_ECHO_CAP_KB` var (adapter-only change).
- Sail/OpenRouter dual body-styles and Gemini OpenAI-compat lines assumed possible per user note; `BodyStyle` sniffing covers them — if a vendor shape falls through, it surfaces as item-level error, never silent corruption.
