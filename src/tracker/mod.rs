//! hyperliquid-trader-tracker: watch wallets over Hyperliquid's public trades feed.

pub mod app;
pub mod book;
pub mod bot;
pub mod config;
pub mod db;
pub mod enrich;
pub mod exceptions;
pub mod hl_client;
pub mod listener;
pub mod models;
pub mod notifier;
pub mod pnl;
pub mod registry;
pub mod resolve;
pub mod retry;
pub mod state;
pub mod telegram_setup;
pub mod watchlist;

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:     src/tracker/__init__.py (1 line)
//   confidence: high
//   todos:      0
//   notes:      crate root (Cargo [lib] path points here); module list mirrors the package
// ──────────────────────────────────────────────────────────────────────────
