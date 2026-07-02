//! Process entrypoint: wire the subscriptions DB, in-memory book, enricher, listener, and bot.
//!
//! Startup ordering preserves cold-start correctness: the listener starts against an empty registry
//! (so it filters everything out harmlessly), then each wallet with a persisted subscriber is seeded
//! from `clearinghouseState` and admitted to the filter only once its seed lands — so no add on a
//! pre-existing position is mis-reported as a new open. This is a multi-tenant bot: one firehose
//! connection serves every subscriber; a lifecycle event fans out to all subscribers of that wallet.
//! Nothing but the per-subscriber watchlists is persisted; the book is rebuilt from the chain on
//! every start.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::FutureExt;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::book::InMemoryBook;
use crate::bot::{Application, SettingsBot, TelegramSender};
use crate::config::{Settings, load_env};
use crate::db::{Subscription, WatchlistDB};
use crate::enrich::Enricher;
use crate::hl_client::{HyperliquidClient, InfoClient};
use crate::listener::Listener;
use crate::notifier::{LoggingSender, MessageSender, Notifier};
use crate::pnl::ClosedPnlResolver;
use crate::registry::Registry;

// PORT NOTE: _amain's failure modes were "any exception propagates out of asyncio.run" —
// the crash-on-startup contract. The enum keeps the sources matchable; the binary main
// prints and exits non-zero.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Config(#[from] crate::config::Error),
    #[error(transparent)]
    Db(#[from] tokio_rusqlite::Error),
    #[error(transparent)]
    Bot(#[from] crate::bot::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

// The long-poll hold Telegram is asked for; PTB's Updater defaulted to 10s.
// PORT NOTE: structural addition — the polling loop lived inside PTB's Updater.
const POLL_TIMEOUT_S: u64 = 10;

async fn sleep_or_stop(stop: &CancellationToken, seconds: f64) {
    // PORT NOTE: `asyncio.wait_for(stop.wait(), timeout=s)` + suppress(TimeoutError) →
    // select over the token and a sleep; both arms just return.
    tokio::select! {
        _ = stop.cancelled() => {}
        _ = tokio::time::sleep(Duration::from_secs_f64(seconds)) => {}
    }
}

fn group_by_address(subs: Vec<Subscription>) -> HashMap<String, Vec<Subscription>> {
    // PORT NOTE: defaultdict(list) → entry().or_default(). Plain HashMap: iteration order
    // only feeds an unordered concurrent seed sweep, so dict insertion order is unobservable.
    let mut grouped: HashMap<String, Vec<Subscription>> = HashMap::new();
    for sub in subs {
        grouped.entry(sub.address.clone()).or_default().push(sub);
    }
    grouped
}

/// Seed each unique wallet once, then admit all its subscribers (seed-before-admit).
async fn seed_and_admit(
    enricher: Arc<Enricher>,
    registry: Arc<Mutex<Registry>>,
    grouped: HashMap<String, Vec<Subscription>>,
    concurrency: usize,
) {
    let sem = Arc::new(Semaphore::new(concurrency));

    // PORT NOTE: the nested `async def _one` + asyncio.gather(*(...)) → per-address futures
    // joined with join_all (no spawn needed: gather ran them on the same loop/task too).
    let futures = grouped.into_iter().map(|(address, members)| {
        let enricher = Arc::clone(&enricher);
        let registry = Arc::clone(&registry);
        let sem = Arc::clone(&sem);
        async move {
            let _permit = sem.acquire().await.expect("semaphore closed");
            if enricher.seed_wallet(&address).await {
                // PORT NOTE: GIL-free — lock scope: the registry lock is taken only after
                // the seed await resolved and is dropped at end of block (no await inside).
                let mut registry = registry.lock().expect("registry mutex poisoned");
                for member in &members {
                    registry.subscribe(member.chat_id, &address, &member.label);
                }
            }
        }
    });
    futures_util::future::join_all(futures).await;
}

/// Periodically (a) seed+admit any persisted-but-untracked wallets (retry failed seeds) and
/// (b) re-seed a rotating batch of tracked wallets to refresh leverage + correct size drift.
async fn reconcile_loop(
    settings: Settings,
    enricher: Arc<Enricher>,
    db: Arc<WatchlistDB>,
    registry: Arc<Mutex<Registry>>,
    stop: CancellationToken,
) {
    if settings.reconcile_interval_s <= 0.0 {
        return;
    }
    let mut cursor: usize = 0;
    while !stop.is_cancelled() {
        sleep_or_stop(&stop, settings.reconcile_interval_s).await;
        if stop.is_cancelled() {
            return;
        }
        // PORT NOTE: the Python wrapped the whole cycle in `except Exception` (a reconcile
        // hiccup must not kill the loop). The only fallible call is db.all(); seed paths
        // swallow their own errors — so the match on db.all() is the whole translation.
        let subs = match db.all().await {
            Ok(subs) => subs,
            Err(err) => {
                tracing::error!("reconcile cycle failed; retrying next interval: {err}");
                continue;
            }
        };
        let pending: HashMap<String, Vec<Subscription>> = {
            // PORT NOTE: GIL-free — lock scope: is_tracked reads happen under one short
            // lock, dropped before the seed awaits below.
            let registry_guard = registry.lock().expect("registry mutex poisoned");
            group_by_address(subs)
                .into_iter()
                .filter(|(addr, _)| !registry_guard.is_tracked(addr))
                .collect()
        };
        if !pending.is_empty() {
            seed_and_admit(
                Arc::clone(&enricher),
                Arc::clone(&registry),
                pending,
                settings.seed_concurrency,
            )
            .await;
        }
        let tracked: Vec<String> = {
            let registry_guard = registry.lock().expect("registry mutex poisoned");
            let mut tracked: Vec<String> = registry_guard.addresses().iter().cloned().collect();
            tracked.sort();
            tracked
        };
        if !tracked.is_empty() {
            let start = cursor % tracked.len();
            // PORT NOTE: Python slice `tracked[start : start+batch]` tolerates an end past
            // len(); Rust must clamp explicitly.
            let end = (start + settings.reconcile_batch).min(tracked.len());
            let batch = tracked[start..end].to_vec();
            cursor = start + settings.reconcile_batch;
            enricher.seed_many(batch).await;
        }
    }
}

/// The Telegram update-polling loop — the port of PTB's `updater.start_polling()` machinery.
// PORT NOTE: structural addition (see bot.rs) — python-telegram-bot owned this loop; here the
// app long-polls getUpdates and feeds each update to SettingsBot::handle_update.
async fn telegram_polling_loop(
    application: Arc<Application>,
    settings_bot: Arc<SettingsBot>,
    stop: CancellationToken,
) {
    let mut offset: Option<i64> = None;
    while !stop.is_cancelled() {
        let updates = tokio::select! {
            _ = stop.cancelled() => return,
            updates = application.bot().get_updates(offset, POLL_TIMEOUT_S) => updates,
        };
        match updates {
            Ok(updates) => {
                for update in updates {
                    if let Some(id) = update.update_id() {
                        offset = Some(offset.map_or(id + 1, |o| o.max(id + 1)));
                    }
                    // A failed handler must not kill the poll loop (PTB logged and moved on).
                    if let Err(err) = settings_bot.handle_update(&update).await {
                        tracing::error!("telegram update handling failed: {err}");
                    }
                }
            }
            Err(err) => {
                tracing::warn!("getUpdates failed ({err}); retrying");
                sleep_or_stop(&stop, 1.0).await;
            }
        }
    }
}

async fn amain(settings: Settings) -> Result<()> {
    let mut db = WatchlistDB::new(settings.db_path.clone());
    db.connect().await?;
    let db = Arc::new(db);
    let book = Arc::new(Mutex::new(InMemoryBook::new()));
    let registry = Arc::new(Mutex::new(Registry::new()));
    let stop = CancellationToken::new();

    // PORT NOTE: `async with HyperliquidClient(settings)` → plain constructor; reqwest
    // clients close on drop, so the context-manager scope is the function body itself.
    let client: Arc<dyn InfoClient> = Arc::new(HyperliquidClient::new(settings.clone()));
    let enricher = Arc::new(Enricher::new(
        settings.clone(),
        Arc::clone(&book),
        Arc::clone(&client),
    ));

    // PORT NOTE: the Python imported telegram lazily inside the `if` (optional extra); the
    // Rust bot module is always compiled — only the wiring is conditional.
    let sender: Arc<dyn MessageSender>;
    let mut application = None;
    let mut settings_bot = None;
    // PORT NOTE: `if settings.telegram_bot_token:` was a TRUTHINESS check — an empty
    // TELEGRAM_BOT_TOKEN fell through to log-only delivery; the filter mirrors that
    // (a bare Some("") would build a bot whose configure() call fails startup).
    if let Some(token) = settings
        .telegram_bot_token
        .clone()
        .filter(|token| !token.is_empty())
    {
        let app = Arc::new(Application::new(token));
        sender = Arc::new(TelegramSender::new(Arc::clone(&app)));
        // PORT NOTE: in Python, *constructing* SettingsBot registered the handlers on the
        // Application; here handler dispatch lives in handle_update, fed by the poll loop.
        settings_bot = Some(Arc::new(SettingsBot::new(
            settings.clone(),
            Arc::clone(&app),
            Arc::clone(&db),
            Arc::clone(&book),
            Arc::clone(&registry),
            Arc::clone(&enricher),
        )?));
        application = Some(app);
    } else {
        tracing::warn!("no TELEGRAM_BOT_TOKEN configured — notifications will be logged only");
        sender = Arc::new(LoggingSender);
    }

    let notifier = Notifier::new(sender, settings.notify_reduce_close);
    // No resolver when close notifications are muted — the lookup's only consumer is the
    // close message, so fetching would just spend REST budget on a dropped event.
    let pnl_resolver = if settings.closed_pnl_lookup && settings.notify_reduce_close {
        Some(ClosedPnlResolver::new(&settings, Arc::clone(&client)))
    } else {
        None
    };
    let mut listener = Listener::new(
        settings.clone(),
        Arc::clone(&registry),
        Arc::clone(&book),
        notifier,
        Arc::clone(&client),
        pnl_resolver,
    );
    let listener_stop = listener.stop_token();

    // PORT NOTE: `loop.add_signal_handler(sig, lambda: (stop.set(), listener.stop()))` →
    // a task selecting on both unix signals; it cancels both tokens, exactly the lambda.
    let signal_task = tokio::spawn({
        let stop = stop.clone();
        let listener_stop = listener_stop.clone();
        async move {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("SIGTERM handler install failed");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = sigterm.recv() => {}
                _ = stop.cancelled() => return, // normal shutdown — stand down
            }
            stop.cancel();
            listener_stop.cancel();
        }
    });

    let mut poll_task = None;
    if let (Some(application), Some(settings_bot)) = (&application, &settings_bot) {
        // PORT NOTE: application.initialize()/start() have no Rust counterpart (no PTB
        // machinery to warm up); configure() registers the slash-command menu, then the
        // poll loop replaces updater.start_polling().
        settings_bot.configure().await?;
        poll_task = Some(tokio::spawn(telegram_polling_loop(
            Arc::clone(application),
            Arc::clone(settings_bot),
            stop.clone(),
        )));
    }

    let subscriptions = db.all().await?;
    let listener_task = tokio::spawn({
        // The listener only returns once stop is set; if it ever exits unexpectedly — by
        // returning OR by panicking — trip stop so the process shuts down cleanly instead
        // of idling forever on `stop.cancelled()`.
        // PORT NOTE: `listener_task.add_done_callback(lambda _t: stop.set())` fired on
        // exceptional completion too; catch_unwind extends the same "done means done"
        // contract to panics. AssertUnwindSafe is sound here because the very next act is
        // process shutdown — no state is reused past the catch (and the shared book/registry
        // mutexes self-poison if they were mid-mutation).
        let stop = stop.clone();
        async move {
            if let Err(panic) = AssertUnwindSafe(listener.run()).catch_unwind().await {
                let msg = panic
                    .downcast_ref::<&str>()
                    .map(|s| (*s).to_string())
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "non-string panic payload".to_string());
                tracing::error!("listener task panicked: {msg}");
            }
            stop.cancel();
        }
    });
    let reconcile_task = tokio::spawn(reconcile_loop(
        settings.clone(),
        Arc::clone(&enricher),
        Arc::clone(&db),
        Arc::clone(&registry),
        stop.clone(),
    ));
    let seed_task = tokio::spawn(seed_and_admit(
        Arc::clone(&enricher),
        Arc::clone(&registry),
        group_by_address(subscriptions),
        settings.seed_concurrency,
    ));

    stop.cancelled().await;

    // PORT NOTE: `for task in tasks: task.cancel()` + gather(return_exceptions=True) →
    // abort + await each handle, swallowing the JoinError a cancellation produces.
    let mut tasks = vec![listener_task, reconcile_task, seed_task];
    if let Some(task) = poll_task {
        tasks.push(task);
    }
    for task in &tasks {
        task.abort();
    }
    for task in tasks {
        let _ = task.await;
    }
    signal_task.abort();
    let _ = signal_task.await;
    // PORT NOTE: `await db.aclose()` — close through the shared handle so queued watchlist
    // writes flush before exit (tokio_rusqlite processes its channel in order); merely
    // dropping the Arc would leave the worker thread racing process teardown. Python let a
    // failing aclose propagate out of the finally; logging is the kinder equivalent since
    // we are exiting either way.
    if let Err(err) = db.close_shared().await {
        tracing::warn!("watchlist DB close failed: {err}");
    }
    Ok(())
}

/// Console-script entry point (`hl-tracker`).
pub fn main() -> ExitCode {
    // PORT NOTE: logging.basicConfig(INFO, "%(asctime)s %(levelname)s %(name)s: %(message)s")
    // → tracing_subscriber's fmt layer (timestamp + level + target by default); RUST_LOG
    // overrides the level like PYTHONLOGLEVEL never could.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    load_env();
    let settings = match Settings::from_env() {
        Ok(settings) => settings,
        Err(err) => {
            eprintln!("invalid configuration: {err}");
            return ExitCode::FAILURE;
        }
    };
    // PORT NOTE: `asyncio.run(_amain(...))` → explicit multi-thread runtime block_on.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime build failed");
    match runtime.block_on(amain(settings)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("hl-tracker failed: {err}");
            ExitCode::FAILURE
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:     src/tracker/app.py (185 lines)
//   confidence: high
//   todos:      0
//   notes:      telegram poll loop is a structural addition (PTB's Updater owned it in
//               Python); application.initialize()/start()/stop()/shutdown() lifecycle
//               collapses into task spawn/abort; db.aclose() replaced by drop.
// ──────────────────────────────────────────────────────────────────────────
