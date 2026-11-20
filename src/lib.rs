pub mod adapters;
pub mod chaos;
pub mod config;
pub mod control;
pub mod coordinator;
pub mod db;
pub mod do_glue;
pub mod domain;
pub mod registry;
pub mod router;
pub mod webhook;

use worker::{Context, Env, Request, Response, event};

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: Context) -> Result<Response, worker::Error> {
    do_glue::handle_edge(req, env).await
}
