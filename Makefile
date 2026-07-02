.PHONY: format lint typecheck test check run lock up down logs \
        format-rs lint-rs test-rs check-rs build-rs run-rs

# This machine leaks the system site-packages onto PYTHONPATH, which shadows the
# venv's typing_extensions; clear it for every recipe so the venv is authoritative.
export PYTHONPATH :=

# python-telegram-bot is a core dependency, so `uv run` installs it into the env
# by default — no `--extra` needed for `ty`/`ruff` to resolve `import telegram`
# in src/tracker/bot.py.

# ---- Checks ----
# Auto-fix imports + lint, then format (the mutating fixer).
format:
	uv run ruff check --fix .
	uv run ruff format .

# Non-mutating lint (CI shape); `make format` is the mutating fixer.
lint:
	uv run ruff check .

# Static type check with ty (https://docs.astral.sh/ty/).
typecheck:
	uv run ty check

# Offline test suite (fixtures + aiosqlite; no network/WS/Telegram).
test:
	uv run pytest -q

# Everything green before shipping: ruff + ty + pytest, plus the Rust port's checks.
check: lint typecheck test check-rs

# ---- Rust port (the .rs files next to each .py; Cargo.toml at the repo root) ----
format-rs:
	cargo fmt --all

# Non-mutating lint (CI shape); `make format-rs` is the mutating fixer.
lint-rs:
	cargo clippy --all-targets -- -D warnings

test-rs:
	cargo test

# Everything green before shipping (Rust): fmt --check + clippy -D warnings + tests.
check-rs: lint-rs test-rs
	cargo fmt --all --check

build-rs:
	cargo build --release

run-rs:
	cargo run --release --bin hl-tracker

# ---- Run ----
# Run the tracker locally against the repo-root .env (log-only without a Telegram token).
run:
	uv run hl-tracker

# Refresh uv.lock after changing dependencies in pyproject.toml.
lock:
	uv lock

# ---- Docker ----
# Tear down any prior deployment of this project (stops + removes its
# container/network), then rebuild and start fresh in the background. Use this
# to pick up code changes — `docker compose restart` alone would NOT rebuild.
up:
	docker compose down --remove-orphans
	docker compose up --build -d

down:
	docker compose down --remove-orphans

# Follow the tracker's logs (its notifications land here in log-only mode).
logs:
	docker compose logs -f
