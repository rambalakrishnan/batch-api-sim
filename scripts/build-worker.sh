#!/usr/bin/env bash
# Production build for Workers Builds, whose image has no Rust toolchain.
# Dashboard: Settings > Build > Build command = `npm run build`.
set -euo pipefail

if ! command -v rustup >/dev/null 2>&1; then
  echo "Installing rustup..."
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --profile minimal --default-toolchain stable
fi
export PATH="$HOME/.cargo/bin:$PATH"
rustup target add wasm32-unknown-unknown

echo "Running unit tests..."
cargo test --locked

if ! command -v worker-build >/dev/null 2>&1; then
  echo "Installing worker-build..."
  cargo install worker-build --version 0.9.0 --locked
fi

worker-build --release
