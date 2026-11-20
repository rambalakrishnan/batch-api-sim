# batch-api-sim — Multi-Provider LLM Batch API Simulator

A single Cloudflare Worker that faithfully simulates the **asynchronous batch APIs** of
OpenAI, OpenRouter, Sail Research, and Google Gemini — so client workers, pollers, and
webhook receivers can be integration-tested in seconds, with zero token spend and
fully reproducible chaos.

**Live:** `https://batch-sim.5900209.xyz` (currently access-restricted; run locally with `wrangler dev`).

```bash
# Upload once, batch, poll to completion — no API keys, no queues, no cost.
curl -X POST localhost:8787/openai/v1/files --data-binary @batch.jsonl
curl -X POST localhost:8787/openai/v1/batches -H 'X-Sim-Delay-Ms: 200' \
  -d '{"input_file_id":"file_...","endpoint":"/v1/chat/completions","completion_window":"24h"}'
curl localhost:8787/openai/v1/batches/batch_...
```

---

## Table of contents

1. [Why this exists](#why-this-exists)
2. [Quickstart](#quickstart)
3. [Provider API reference](#provider-api-reference)
4. [Control plane: headers & environment](#control-plane-headers--environment)
5. [Chaos engineering guide](#chaos-engineering-guide)
6. [Webhooks](#webhooks)
7. [Quotas, auth & limits](#quotas-auth--limits)
8. [Architecture](#architecture)
9. [Design decisions](#design-decisions)
10. [Project layout](#project-layout)
11. [Testing](#testing)
12. [Deployment](#deployment)

---

## Why this exists

Real batch APIs are hostile to integration tests: OpenAI batches take minutes–hours,
OpenRouter up to 24h, Gemini up to 24h — and every test run burns tokens. The usual
workarounds (mock HTTP stubs, recorded fixtures) rot the moment a vendor tweaks an
envelope, and they can't exercise async polling, retries, cancellation races, or
webhook receivers.

batch-api-sim replaces all of that with one stateful mock server that is:

- **Provider-accurate on the wire** — response envelopes transcribed from vendor docs
  (OpenAI batch objects, OpenRouter `BatchObject`, Gemini LRO operations), verified by
  offline fixture tests.
- **Async for real** — batches move `validating → in_progress → completed|failed`
  on Durable Object alarms with jittered delays, so pollers observe genuine transitions.
- **Deterministically chaotic** — seeded RNG drives batch failures, per-item 429/500s,
  and edge 429/503s. Same seed ⇒ byte-identical outcomes, every run.
- **Zero-dependency ops** — one Worker, two SQLite-backed Durable Objects, no R2/KV/D1.

## Quickstart

Prerequisites: Rust stable + `wasm32-unknown-unknown` target, `worker-build 0.9`,
Node 20+ with the repo's `wrangler` devDependency.

```bash
git clone https://github.com/rambalakrishnan/batch-api-sim.git
cd batch-api-sim
npm install
npx wrangler dev --port 8787   # builds? no — build first:
```

Rust workers need an explicit bundle step before `wrangler dev` picks them up:

```bash
worker-build          # dev bundle → build/
npx wrangler dev      # serves localhost:8787 with local DOs
```

Smoke test (OpenRouter inline batch, 100 ms simulated queue):

```bash
curl -X POST localhost:8787/openrouter/api/v1/batches \
  -H 'Content-Type: application/json' \
  -H 'X-Sim-Delay-Ms: 100' \
  -d '{"endpoint":"/v1/chat/completions","model":"openai/gpt-4o",
       "requests":[{"custom_id":"r1","body":{"messages":[]}}]}'
# → 202 {"id":"batch_...","object":"batch","status":"validating",...}

curl localhost:8787/openrouter/api/v1/batches/batch_...
# → 200 {"status":"completed","results":[{"custom_id":"r1",
#     "response":{"body":{"object":"chat.completion",...}},...}],...}
```

## Provider API reference

Four isolated URL prefixes, one shared engine. All JSON unless noted.
`{bid}` = batch id, `{fid}` = file id.

### OpenAI — `/openai/v1/*`

| Method & path | Purpose |
|---|---|
| `POST /openai/v1/files` | Upload `.jsonl` (`multipart/form-data` `file` part, or raw body). → `{"id":"file_…","object":"file","bytes","created_at","filename","purpose":"batch"}` |
| `POST /openai/v1/batches` | `{"input_file_id","endpoint","completion_window"}` → 200 batch object, `status: validating` |
| `GET /openai/v1/batches/{bid}` | Batch object with `request_counts`, `output_file_id`/`error_file_id` when complete |
| `POST /openai/v1/batches/{bid}/cancel` | → `cancelled`, or `409` if already terminal |
| `GET /openai/v1/files/{fid}/content` | `application/jsonl` — output lines, error lines, or the original upload echo |

JSONL lines: `{"custom_id","method","url","body"}` (`url` defaults to
`/v1/chat/completions`). Output lines mirror the real shape —
`{"custom_id","response":{"status_code","request_id","body"},"error":null}` —
with failures routed to the error file.

### OpenRouter — `/openrouter/api/v1/*`

Shapes follow the [OpenRouter Batch API reference](https://openrouter.ai/docs/api/api-reference/batch/create-a-batch).

| Method & path | Purpose |
|---|---|
| `POST /openrouter/api/v1/batches` | `{"endpoint" (enum, required),"model" (required),"requests":[{"custom_id","body"}],"callback_url"?}` → **202** `BatchObject` |
| `GET /openrouter/api/v1/batches/{bid}` | `BatchObject`; `results` embedded once terminal, else `null` |
| `POST /openrouter/api/v1/batches/{bid}/cancel` | Simulator extension (uniform lifecycle) |

`endpoint` must be one of `/v1/chat/completions`, `/v1/responses`, `/v1/messages`,
`/v1/embeddings`, `/v1/audio/speech`. Errors use the native `{error:{code:int,message}}`
envelope. `callback_url` must be `https://` (native validation); use
`X-Sim-Callback-Url` for `http://` test receivers (see [Webhooks](#webhooks)).

### Sail Research — `/sail/v1/*`

Simulator-defined vendor (no public wire docs): OpenAI-style envelopes, inline requests.

| Method & path | Purpose |
|---|---|
| `POST /sail/v1/batches` | `{"endpoint"? (default /v1/responses),"requests":[{"custom_id","params"}]}` — **`body` is rejected with 400; `params` is enforced** |
| `GET /sail/v1/batches/{bid}` | Batch object, inline `results` when terminal |
| `GET /sail/v1/batches/{bid}/{custom_id}` | Per-item result; `404` while pending or unknown |
| `POST /sail/v1/batches/{bid}/cancel` | Simulator extension |

### Gemini — `/gemini/v1beta/*`

Shapes follow the [Gemini Batch API guide](https://ai.google.dev/gemini-api/docs/batch-api).

| Method & path | Purpose |
|---|---|
| `POST /gemini/upload/v1beta/files` (alias: `/upload/v1beta/files`) | Upload JSONL (raw or `multipart/related`) → `{"file":{"name":"files/…",…}}` |
| `POST /gemini/v1beta/models/{model}:batchGenerateContent` | `{"batch":{"display_name"?,"input_config":{"requests":{"requests":[{"request","metadata":{"key"}}]}} \| {"file_name":"files/…"}}}`
| `GET /gemini/v1beta/batches/{bid}` (alias: `/gemini/v1beta/operations/{bid}`) | Long-Running Operation: `{name:"batches/…",metadata:{"@type",state},done,response?|error?}` |
| `POST /gemini/v1beta/batches/{bid}:cancel` (alias under `/operations/`) | → `{}`; terminal race → `400 FAILED_PRECONDITION` |
| `GET /gemini/download/v1beta/files/{fid}:download` | Result file download (`responsesFile` target) |

State mapping: `validating→JOB_STATE_PENDING`, `in_progress→JOB_STATE_RUNNING`,
`completed→JOB_STATE_SUCCEEDED`, `failed→JOB_STATE_FAILED` (+ `google.rpc.Status`
`error`), `cancelled→JOB_STATE_CANCELLED`. Completed responses inline
`inlinedResponses` up to 256 KiB serialized, else `responsesFile`. Errors use
`{error:{code:int,message,status}}` with `INVALID_ARGUMENT / UNAUTHENTICATED /
NOT_FOUND / FAILED_PRECONDITION / RESOURCE_EXHAUSTED / INTERNAL / UNAVAILABLE`.

File lines: `{"key","request"}` where `request` is a `GenerateContentRequest`
(or OpenAI-compatible `{messages}` — see per-item sniffing below).

## Control plane: headers & environment

Every knob is a request header; every default is a `SIM_*` var
(`wrangler.toml` `[vars]`). Invalid control values silently fall back to defaults
— the control plane never 400s. Integers are clamped to 2⁵³ (JS-safe).

| Header | Default | Effect |
|---|---|---|
| `X-Sim-Delay-Ms` | 30000 / 300000 | Overrides **both** validation & processing delays. `0` fast-forwards (alarm fires ASAP) |
| `X-Sim-Fail-Rate` | `0.0` | P(batch aborts to `failed` at first transition) |
| `X-Sim-Item-Fail-Rate` | `0.0` | P(each item errors independently at completion) |
| `X-Sim-Edge-Chaos-Rate` | `0.0` | P(each `GET` poll returns transient `429`+`Retry-After: 1` (70%) or `503`) |
| `X-Sim-Seed` | random | u64 pinning **all** of a batch's dice rolls; echoed back as `X-Sim-Seed-Used` |
| `X-Sim-Callback-Url` | — | Webhook URL for **any** provider (test extension; beats native `callback_url`) |
| `X-Sim-Webhook-Secret` | — | HMAC-SHA256 secret; absent ⇒ unsigned delivery |

Environment mirrors: `SIM_VALIDATION_DELAY_MS`, `SIM_PROCESSING_DELAY_MS`,
`SIM_JITTER_PCT` (default 50, ±%), `SIM_BATCH_FAIL_RATE`, `SIM_ITEM_FAIL_RATE`,
`SIM_EDGE_CHAOS_RATE`, plus `SIM_REQUIRE_AUTH` / `SIM_API_KEYS` (§7).

Unknown `X-Sim-*` headers are ignored.

## Chaos engineering guide

**Seeded determinism.** Each batch derives independent RNG streams from its seed:
creation jitter, alarm-1 (batch-fail roll + processing jitter), per-item outcome
rolls, and edge-chaos rolls each get `seed ⊕ tag ⊕ index`. Same seed + same
request ⇒ byte-identical statuses, outcomes, error codes, and poll flakiness —
proven by the `seed_replay_is_identical` unit test and E2E seed-replay checks.
Omit the seed and the worker picks one and echoes it in `X-Sim-Seed-Used`, so any
run can be replayed exactly.

**Failure injection.**

```bash
# Catastrophic batch failure with a provider-accurate envelope:
-H 'X-Sim-Fail-Rate: 1.0'
# Every item errors, seeded 50/50 429 (rate_limit) vs 500 (internal):
-H 'X-Sim-Item-Fail-Rate: 1.0'
# Polls flap until your client backs off:
-H 'X-Sim-Edge-Chaos-Rate: 1.0'   # 429s carry Retry-After: 1
```

Edge rolls run on their own stream, so polling never consumes outcome RNG —
chaos stays reproducible no matter how often you poll.

**Per-item body dialects.** Each request unit is sniffed independently —
explicit endpoint match first (`responses`, `chat/completions`, `generateContent`,
`embeddings`), then payload shape (`messages[]`→chat, `input[]`/`instructions`→responses,
`contents[]`→Gemini) — so one batch can mix Chat Completions, Responses, Gemini-native,
and Embeddings items, each rendered into its own schema-valid skeleton. Empty/non-object
bodies become item-level `400` errors at completion, never batch failures.

**Success bodies** are minimal valid envelopes (`sim-mock-1`, zero usage,
`sim-echo:{custom_id}` content) plus a deep echo of the normalized input under a
marked `sim_echo` field, truncated at 8 KiB with `"truncated": true`.

## Webhooks

Only OpenRouter has native batch webhooks (`callback_url` in the create body);
the simulator adds a uniform `X-Sim-Callback-Url` extension header on all four
providers. Payloads stay wire-shaped per provider (OpenRouter `{id,event,data}`,
OpenAI/Sail event envelopes, Gemini operation + `simEvent`).

Delivery policy (locked): Alarm #2 POSTs inline with **3 attempts** (500 ms / 2 s
backoff), then records `webhook_status: delivered|failed` on the batch — no alarm
rescheduling. Headers: `Dedupe-Key: {batch_id}:{status}` (alarms are at-least-once;
dedupe on this) and `X-Webhook-Signature: sha256=<hex HMAC-SHA256>` only when a
secret is set. Payloads embed at most 100 items; receivers `GET` the rest.

## Quotas, auth & limits

| Guard | Value | Behavior |
|---|---|---|
| Items / batch | 10,000 | Synchronous provider-shaped `400` at create |
| Upload size | 32 MiB | `400` at upload |
| JSONL line | 1 MiB | `400` naming the line |
| Registry retention | 24 h unreferenced / 7 d referenced | Hourly prune sweep in `FileRegistry` |

**Auth:** permissive by default (any key or none passes). Set `SIM_REQUIRE_AUTH=1`
+ `SIM_API_KEYS=k1,k2,…` to enforce an allowlist with provider-shaped `401`s —
verified live (`UNAUTHENTICATED` on Gemini).

## Architecture

```
[Client poller / webhook receiver]
       │
       ▼
┌──────────────────────────────────────────────────────────────┐
│ Edge Worker ── prefix router + auth + control headers        │
│  /openai/v1/*  /openrouter/api/v1/*  /sail/v1/*  /gemini/*    │
│  └─ Provider adapters: ingest (wire→Unified*) /              │
│     render_status / render_results / build_webhook           │
└────────────────────────┬─────────────────────────────────────┘
                         │ internal JSON RPC over DO stubs
        ┌────────────────┴───────────────┐
        ▼                                ▼
┌──────────────────────┐      ┌──────────────────────────────┐
│ FileRegistry (×1)    │      │ BatchCoordinator (×batch)    │
│ uploads only, TTL    │ copy│ items + outcomes + outputs   │
│ alarm: hourly prune  │ ───►│ alarms: validate → complete  │
└──────────────────────┘     └──────────────────────────────┘
        SQLite-backed Durable Objects. No R2/KV/D1.
```

**Lifecycle.** `POST /batches` synchronously validates, copies file rows into the
coordinator (**copy-at-create**), and schedules Alarm #1. Alarm #1 rolls
batch-failure and schedules Alarm #2; Alarm #2 materializes per-item outcomes,
embeds output artifacts as SQLite rows, and fires the webhook. Alarms never perform
cross-DO I/O — crash/retry surface is confined to one SQLite database.

**Module map** (`src/`): `domain` (canonical types) · `chaos` (seeded engine) ·
`control` (headers/env/auth) · `config` (locked constants) · `db` (SQL trait +
`WorkerDb` / rusqlite test backend) · `registry` + `coordinator` (stores +
internal protocols; `BatchPort`/`FileSource` traits) · `adapters/{common,openai,
openrouter,sail,gemini}` (wire shapes + fixtures) · `router` (pure matcher +
upload parsing) · `webhook` (HMAC + dispatch) · `do_glue` (DO structs, stub RPC,
edge handler).

## Design decisions

**Two DO classes, not one, not N.** Per-file DOs (the naive mapping) can't be
enumerated or GC'd — orphan sprawl against the 5 GB cap. A single class for
everything muddles schemas and lifecycles. The split actually used — one
`FileRegistry` singleton for uploads with TTL sweeps, one `BatchCoordinator` per
batch that owns everything after creation — gives file reuse *and* trivial GC.
Singleton write contention is a non-issue at integration-test traffic, with a
documented 5-line shard fallback.

**Copy-at-create.** Resolving `FileRef`s inside Alarm #1 (the first design) put
cross-DO RPC on the failure-prone alarm path and deferred `file_id` validation.
Copying rows synchronously during `POST /batches` fails fast with a 404/400,
keeps every alarm a local SQLite transaction, and bounds edge memory via 256-row
paged reads. The one exception the async boundary forces: the coordinator's
`fetch` handler pages the registry over stub RPC, then submits through the same
sync, unit-tested store path — one code path, two transports.

**Embedded outputs.** Output/error artifacts live as rows in the owning batch's
SQLite; `file-out-{batch_id}` encodes the owner so content fetches route with zero
lookup. No second FileStore write, no lifecycle skew.

**Compile-time provider exhaustiveness.** `ProviderKind` is an opaque tag in core;
`adapter_for()` matches it with no wildcard arm, so adding a variant breaks the
build until `ingest`, `render_status`, `render_results`, `render_item`,
`build_webhook`, `render_error` (+ HTTP status hooks) all exist. Core literally
cannot branch on providers — the type system enforces the hexagon.

**Determinism architecture.** No RNG state is persisted. Every roll derives
`ChaosEngine(seed ⊕ stream_tag ⊕ index)`, so at-least-once alarm redelivery and
arbitrary poll counts can't perturb outcomes. `fastrand`'s `js` feature is
enabled — without it, wasm falls back to a *fixed* seed and batch ids collide
across restarts (found the hard way: a 409 on redeploy).

**u64s vs the JS boundary.** `Request::json()` routes through f64, so integers
above 2⁵³ arrive mangled. Internal DO RPC therefore parses raw text with
`serde_json`, and control integers are clamped to the JS-safe range with the
effective seed echoed back.

**`set_alarm` takes an offset.** `worker::ScheduledTime: From<i64>` is
*milliseconds-from-now*, not epoch time — passing absolute timestamps schedules
alarms millions of years out, with zero errors. All call sites pass delays now
(this produced the most confusing green-then-stuck symptom of the build).

## Project layout

```
├── src/                    # worker crate (workers-rs 0.9, wasm32)
├── scripts/build-worker.sh # Workers Builds: rustup + wasm32 + test + bundle
├── wrangler.toml           # DO bindings, v1 sqlite migration, SIM_* defaults
├── package.json            # wrangler 4.x + npm run build
└── Cargo.toml / Cargo.lock
```

## Testing

- `cargo test` — **41 tests**: adapter fixture round-trips per provider, chaos
  determinism (seed replay, stream independence), `BodyStyle` precedence, store
  logic against real SQLite (rusqlite), quotas, TTL pruning, cancel races.
- Live E2E against `wrangler dev` (throwaway scripts, since removed): all four
  providers upload→create→poll→content; chaos matrix (fail 1.0, item-fail mix,
  edge-chaos + `Retry-After`, seed replay); webhook HMAC/dedupe/retry-then-deliver
  against a capture server; cancel silencing; quota/auth edges.
- `cargo build --target wasm32-unknown-unknown` — zero warnings; release bundle
  323 KiB gzipped (1 MiB free budget).

## Deployment

Dashboard Git integration (**Workers Builds**): Settings → Build →
**Build command** `npm run build`, deploy command default `npx wrangler deploy`.
The build image has no Rust toolchain, so `scripts/build-worker.sh` installs
rustup + `wasm32-unknown-unknown` + `worker-build 0.9`, gates on `cargo test`,
and emits `build/` for wrangler. First build takes ~6–9 min; no API token or
manual Worker/DO provisioning needed — the `v1` migration in `wrangler.toml`
creates both Durable Object classes on first deploy.
