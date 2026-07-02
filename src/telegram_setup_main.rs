// PORT NOTE: was the `hl-tracker-telegram-setup = "tracker.telegram_setup:main"`
// console-script entry. The Python main was sync (httpx.Client); the port is async
// (reqwest) — see telegram_setup.rs.
fn main() -> std::process::ExitCode {
    // PORT NOTE: load .env BEFORE the runtime exists — config::load_env's set_var is only
    // sound single-threaded (see its SAFETY note). Python only read .env when no CLI token
    // was given; reading it unconditionally here is a harmless superset (the CLI token
    // still wins inside run()), and run()'s own load_env call then finds every key set.
    tracker::config::load_env();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime build failed")
        .block_on(tracker::telegram_setup::run(std::env::args().collect()))
}
