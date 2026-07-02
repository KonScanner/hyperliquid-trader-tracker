//! SQLite persistence for per-subscriber watchlists — the ONLY thing this project stores.
//!
//! A single `subscriptions(chat_id, address, label)` table keyed by `(chat_id, address)`: each
//! Telegram subscriber keeps their own labelled list of wallets. Addresses are stored lowercase
//! (the caller normalizes). WAL + a busy timeout keep concurrent command/reconcile writes from
//! tripping `SQLITE_BUSY`. No trade/position/PnL data is ever written here.

// TODO(port): error unification — the fixed crate-level `TrackerError` (Parse / RateLimited /
// AuthRequired) has no variant for SQLite failures, and the Python let sqlite3 exceptions
// propagate untyped (nothing in the codebase catches them). This module therefore returns
// `tokio_rusqlite::Error` directly; Phase B must either add a `Db` variant to `TrackerError`
// or bless the raw error type at the app boundary.

use std::collections::HashMap;
use std::path::PathBuf;

use tokio_rusqlite::Connection;

// PORT NOTE: Python module-private `_SCHEMA` → non-pub `SCREAMING_SNAKE_CASE` const (Rust
// privacy replaces the leading-underscore convention).
const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS subscriptions (
    chat_id INTEGER NOT NULL,
    address TEXT    NOT NULL,
    label   TEXT    NOT NULL,
    PRIMARY KEY (chat_id, address)
) STRICT;
";

/// One subscriber tracking one wallet, under their own label.
// PORT NOTE: `@dataclass(frozen=True, slots=True)` — per the fixed port decisions this is a
// plain `#[derive(Debug, Clone, PartialEq)]` struct (no serde; frozen-ness is expressed by
// immutable bindings). The frozen dataclass would also suggest Eq/Hash, but no call site
// hashes Subscriptions, so the fixed derive set stands.
#[derive(Debug, Clone, PartialEq)]
pub struct Subscription {
    pub chat_id: i64,
    pub address: String,
    pub label: String,
}

/// Async CRUD over the `subscriptions` table.
pub struct WatchlistDB {
    // PORT NOTE: Python `_path`/`_conn` → private fields without the underscore prefix.
    path: PathBuf,
    conn: Option<Connection>,
}

impl WatchlistDB {
    // PORT NOTE: `__init__(self, path: Path)` — takes an owned `PathBuf` because it is stored.
    pub fn new(path: PathBuf) -> Self {
        Self { path, conn: None }
    }

    /// Open the connection, apply pragmas, and create the table if absent.
    pub async fn connect(&mut self) -> tokio_rusqlite::Result<()> {
        let conn = Connection::open(&self.path).await?;
        conn.call(|conn| {
            // PORT NOTE: the Python ran `conn.execute("PRAGMA ...")`; rusqlite's `execute`
            // errors on pragmas that return a row (journal_mode / busy_timeout do), so the
            // dedicated `pragma_update` is used — same statements, same effect.
            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "busy_timeout", 5000)?;
            conn.pragma_update(None, "synchronous", "NORMAL")?;
            // PORT NOTE: `conn.row_factory = aiosqlite.Row` vanishes — rusqlite rows are
            // already addressable by column name via `row.get("name")`.
            conn.execute_batch(SCHEMA)?;
            // PORT NOTE: `await conn.commit()` vanishes — rusqlite runs in autocommit mode
            // (no implicit transaction like Python's sqlite3 driver), so DDL is durable here.
            Ok(())
        })
        .await?;
        self.conn = Some(conn);
        Ok(())
    }

    // PORT NOTE: `@property _db` raised RuntimeError("WatchlistDB used before connect()") —
    // a programmer error, so per the fixed port decisions it panics via `expect` instead of
    // returning a Result.
    fn db(&self) -> &Connection {
        self.conn
            .as_ref()
            .expect("WatchlistDB used before connect()")
    }

    /// Return every subscription across all subscribers (for startup load).
    ///
    /// Addresses are lower-cased defensively: the listener's filter requires lowercase (it
    /// lowercases each trade's counterparties), and this keeps that invariant true even if a row
    /// was written out-of-band, without trusting the writer.
    // PORT NOTE: Python shadows the builtin `all`; `all` is not reserved in Rust, so the name
    // carries over unchanged.
    pub async fn all(&self) -> tokio_rusqlite::Result<Vec<Subscription>> {
        self.db()
            .call(|conn| {
                let mut stmt = conn.prepare("SELECT chat_id, address, label FROM subscriptions")?;
                let rows = stmt
                    .query_map([], |r| {
                        Ok(Subscription {
                            chat_id: r.get("chat_id")?,
                            address: r.get::<_, String>("address")?.to_lowercase(),
                            label: r.get("label")?,
                        })
                    })?
                    .collect::<Result<Vec<_>, rusqlite::Error>>()?;
                Ok(rows)
            })
            .await
    }

    /// Return one subscriber's watchlist as `{address: label}`.
    // PORT NOTE: plain HashMap, not IndexMap — the only caller that iterates this
    // (`bot._show_list`) sorts the items first, so Python's dict insertion order is never
    // observable.
    pub async fn list_for(&self, chat_id: i64) -> tokio_rusqlite::Result<HashMap<String, String>> {
        self.db()
            .call(move |conn| {
                let mut stmt =
                    conn.prepare("SELECT address, label FROM subscriptions WHERE chat_id = ?")?;
                let rows = stmt
                    .query_map([chat_id], |r| {
                        Ok((r.get::<_, String>("address")?, r.get::<_, String>("label")?))
                    })?
                    .collect::<Result<HashMap<_, _>, rusqlite::Error>>()?;
                Ok(rows)
            })
            .await
    }

    // INVARIANT: every write below is a single DML + commit. The connection is shared with the
    // reconcile loop, so a MULTI-statement write here could have its partial DML committed by an
    // unrelated coroutine's commit(); never make these multi-statement without serializing them
    // under a private asyncio.Lock or a dedicated write connection.
    // PORT NOTE: kept verbatim per the fixed port decisions. In the Rust port the hazard shape
    // changes — rusqlite autocommits each statement and tokio_rusqlite serializes all `call`s
    // onto one background thread — but the rule stands: a multi-statement write must become an
    // explicit transaction inside ONE `call` closure, never split across closures.

    /// Insert or relabel one subscriber's wallet (idempotent upsert).
    // PORT NOTE: &str params are copied to owned Strings because the `call` closure must be
    // Send + 'static (it runs on tokio_rusqlite's background thread).
    pub async fn add(
        &self,
        chat_id: i64,
        address: &str,
        label: &str,
    ) -> tokio_rusqlite::Result<()> {
        let (address, label) = (address.to_owned(), label.to_owned());
        self.db()
            .call(move |conn| {
                // PORT NOTE: Python's `async with ... : pass` only closed the cursor — the
                // statement handle drops at scope end here. The trailing `commit()` vanishes
                // (autocommit; see connect()).
                conn.execute(
                    "INSERT INTO subscriptions (chat_id, address, label) VALUES (?, ?, ?) \
                     ON CONFLICT(chat_id, address) DO UPDATE SET label = excluded.label",
                    rusqlite::params![chat_id, address, label],
                )?;
                Ok(())
            })
            .await
    }

    /// Remove one subscriber's wallet. Returns `true` if a row was deleted.
    pub async fn delete(&self, chat_id: i64, address: &str) -> tokio_rusqlite::Result<bool> {
        let address = address.to_owned();
        self.db()
            .call(move |conn| {
                // PORT NOTE: `cur.rowcount > 0` → rusqlite `execute` returns the affected
                // row count directly.
                let deleted = conn.execute(
                    "DELETE FROM subscriptions WHERE chat_id = ? AND address = ?",
                    rusqlite::params![chat_id, address],
                )? > 0;
                Ok(deleted)
            })
            .await
    }

    /// Relabel one subscriber's wallet. Returns `true` if it existed.
    pub async fn rename(
        &self,
        chat_id: i64,
        address: &str,
        label: &str,
    ) -> tokio_rusqlite::Result<bool> {
        let (address, label) = (address.to_owned(), label.to_owned());
        self.db()
            .call(move |conn| {
                let renamed = conn.execute(
                    "UPDATE subscriptions SET label = ? WHERE chat_id = ? AND address = ?",
                    rusqlite::params![label, chat_id, address],
                )? > 0;
                Ok(renamed)
            })
            .await
    }

    // PORT NOTE: Python cleared `_conn` only after a successful close (a failing close left it
    // set); `Connection::close` consumes the handle, so `take()` clears it up front — on error
    // the connection is gone either way here. Divergence is intentional: nothing retries aclose.
    pub async fn aclose(&mut self) -> tokio_rusqlite::Result<()> {
        if let Some(conn) = self.conn.take() {
            conn.close().await?;
        }
        Ok(())
    }

    /// Close the worker connection through a shared handle.
    ///
    /// Used at app shutdown, where the DB sits behind an `Arc` and `aclose(&mut self)` is
    /// unreachable. tokio_rusqlite processes its command channel in order, so every queued
    /// write lands before the close; any call after this fails with `ConnectionClosed`,
    /// matching aiosqlite's used-after-close error.
    // PORT NOTE: structural addition for `await db.aclose()` in app.py's finally block —
    // `Connection` is a cloneable channel handle, so closing a clone closes the worker.
    pub async fn close_shared(&self) -> tokio_rusqlite::Result<()> {
        if let Some(conn) = &self.conn {
            conn.clone().close().await?;
        }
        Ok(())
    }
}

// SQLite per-subscriber watchlist persistence CRUD.
#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::Path;

    use super::{Subscription, WatchlistDB};

    // PORT NOTE: pytest's `tmp_path` fixture → `tempfile::tempdir()` created in each test and
    // passed in, matching the `_open(tmp_path)` helper shape.
    async fn _open(tmp_path: &Path) -> WatchlistDB {
        let mut db = WatchlistDB::new(tmp_path.join("tracker.db"));
        db.connect().await.expect("connect");
        db
    }

    fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        // PORT NOTE: helper for the Python dict-literal assertions ({"0xa": "Whale", ...}).
        pairs
            .iter()
            .map(|(a, l)| (a.to_string(), l.to_string()))
            .collect()
    }

    #[tokio::test]
    async fn test_subscriptions_are_scoped_per_chat() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db = _open(tmp.path()).await;
        // PORT NOTE: the Python `try/finally: aclose()` guaranteed close even on assert
        // failure; in Rust a panicking test simply drops the connection (tokio_rusqlite's
        // Drop closes the background thread), so aclose() runs once at the end instead.
        db.add(1, "0xa", "Whale").await.unwrap();
        db.add(1, "0xb", "Other").await.unwrap();
        db.add(2, "0xa", "MyWhale").await.unwrap(); // different subscriber, same wallet, own label
        assert_eq!(
            db.list_for(1).await.unwrap(),
            map(&[("0xa", "Whale"), ("0xb", "Other")])
        );
        assert_eq!(db.list_for(2).await.unwrap(), map(&[("0xa", "MyWhale")]));

        let every = db.all().await.unwrap();
        assert_eq!(every.len(), 3);
        assert!(every.contains(&Subscription {
            chat_id: 1,
            address: "0xa".to_string(),
            label: "Whale".to_string(),
        }));
        assert!(every.contains(&Subscription {
            chat_id: 2,
            address: "0xa".to_string(),
            label: "MyWhale".to_string(),
        }));

        let mut db = db;
        db.aclose().await.unwrap();
    }

    #[tokio::test]
    async fn test_upsert_rename_delete_are_per_chat() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db = _open(tmp.path()).await;
        db.add(1, "0xa", "Whale").await.unwrap();
        db.add(2, "0xa", "MyWhale").await.unwrap();

        db.add(1, "0xa", "relabel-via-upsert").await.unwrap(); // ON CONFLICT relabels chat 1 only
        assert_eq!(db.list_for(1).await.unwrap()["0xa"], "relabel-via-upsert");
        assert_eq!(db.list_for(2).await.unwrap()["0xa"], "MyWhale");

        assert!(db.rename(1, "0xa", "Whale-One").await.unwrap());
        assert!(!db.rename(9, "0xa", "nope").await.unwrap());

        assert!(db.delete(1, "0xa").await.unwrap()); // only chat 1's subscription
        assert!(!db.delete(1, "0xa").await.unwrap());
        assert_eq!(db.list_for(1).await.unwrap(), HashMap::new());
        assert_eq!(db.list_for(2).await.unwrap(), map(&[("0xa", "MyWhale")])); // chat 2 unaffected

        let mut db = db;
        db.aclose().await.unwrap();
    }

    #[tokio::test]
    async fn test_subscriptions_persist_across_reopen() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mut db = _open(tmp.path()).await;
        db.add(1, "0xa", "Whale").await.unwrap();
        db.aclose().await.unwrap();

        let mut reopened = _open(tmp.path()).await;
        assert_eq!(
            reopened.list_for(1).await.unwrap(),
            map(&[("0xa", "Whale")])
        );
        reopened.aclose().await.unwrap();
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:     src/tracker/db.py (113 lines) + tests/test_db.py (61 lines)
//   confidence: medium
//   todos:      1
//   notes:      returns tokio_rusqlite::Error (TrackerError has no Db variant — see TODO at
//               top); verify tokio-rusqlite version's `call` closure signature (0.5+: closure
//               returns tokio_rusqlite::Result, rusqlite::Error converts via From/?).
// ──────────────────────────────────────────────────────────────────────────
