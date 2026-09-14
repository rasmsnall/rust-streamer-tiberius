//! Opening a Firebird connection, and keeping secrets out of everything that can fail.
//!
//! # Why this module is synchronous underneath, unlike tiberiusdelta's
//!
//! `rsfbclient` (this crate's chosen client, see `CLAUDE.md`'s Dependency budget) has a
//! **synchronous** API: every call blocks the calling thread on network I/O, unlike
//! `tiberius` in the sibling `tiberiusdelta` crate. There is no async Firebird wire-
//! protocol client to reach for instead (see `CLAUDE.md`'s "Why `rsfbclient`, not a
//! native driver install"). This crate still needs a Tokio runtime for the Delta write
//! path (`crate::merge`, `crate::checkpoint`), so [`open_blocking`] is the real,
//! synchronous connection logic, and [`open`] is a thin async wrapper that runs it on a
//! blocking-pool thread via `tokio::task::spawn_blocking`. This is the same structural
//! shape `tiberiusdelta`'s history records for *its own* earlier ODBC-based version,
//! before it switched to the native async `tiberius` client; `firebirddelta` inherits
//! that shape for the same underlying reason (a synchronous client), not by choice.
//!
//! # Why this module exists, not just a call to `Connection::open`
//!
//! A connection failure's message can echo back the connection string it was given
//! verbatim, password included (an unparseable-URL diagnostic, or similar).
//! [`Error::Connect`] must never carry that, because it reaches Python tracebacks and, if
//! a caller logs the exception, a log file. Every error this module produces is passed
//! through this module's own (private) redaction step first, the same discipline
//! `tiberiusdelta::connect` applies to its ADO.NET-style string.

use std::fmt;

use rsfbclient::{Connection, FbError};
use rsfbclient_rust::RustFbClient;

use crate::error::{Error, Result};

/// An open Firebird session, using this crate's chosen pure-Rust wire-protocol client
/// (see `CLAUDE.md`'s Dependency budget).
///
/// Named once here, the same way `tiberiusdelta::connect::SqlClient` is, so the concrete
/// client type parameter does not have to be spelled out at every call site.
pub type FbConnection = Connection<RustFbClient>;

/// Settings for opening one Firebird connection.
///
/// Deliberately not `#[derive(Debug)]`: see the hand-written [`fmt::Debug`] impl below,
/// which redacts [`ConnectConfig::connection_string`] so an incidental `{:?}` (a panic
/// message, a debug log elsewhere in a caller's code) cannot leak it.
#[derive(Clone)]
pub struct ConnectConfig {
    /// A `firebird://{user}:{pass}@{host}:{port}/{db_name}?charset=...` connection URL,
    /// `rsfbclient`'s own "one opaque connection string" shape (see
    /// `rsfbclient::builders::PureRustConnectionBuilder::from_string`), mirroring how
    /// `tiberiusdelta::ConnectConfig::connection_string` carries an ADO.NET string.
    /// Carries the password; that is unavoidable; what this module guarantees is that
    /// it never escapes into an error message.
    pub connection_string: String,
    /// Seconds allowed to establish the connection (TCP connect plus Firebird login)
    /// before giving up on waiting for it.
    ///
    /// Enforced by racing [`open`]'s `spawn_blocking` task against
    /// `tokio::time::timeout`, since `rsfbclient` has no login-timeout option of its own
    /// and its socket call is not itself cancellable. `None` waits as long as the
    /// operating system's own TCP timeout, which is typically minutes.
    ///
    /// **This bounds how long the caller waits, not the underlying blocking thread.**
    /// Unlike `tokio::net::TcpStream::connect` (which `tiberiusdelta` can cancel
    /// outright by dropping its future), a timed-out `spawn_blocking` task is only
    /// abandoned: the OS thread it runs on keeps blocking on the socket call until the
    /// operating system's own TCP timeout eventually releases it. A login that never
    /// completes therefore leaks one blocked thread from Tokio's blocking pool per
    /// attempt rather than being cleanly cancelled; see `CLAUDE.md`'s Open items.
    pub login_timeout_sec: Option<u64>,
}

impl fmt::Debug for ConnectConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectConfig")
            .field("connection_string", &"[redacted]")
            .field("login_timeout_sec", &self.login_timeout_sec)
            .finish()
    }
}

/// Opens a connection described by `config`, blocking the calling thread.
///
/// This issues no statement of any kind beyond the Firebird login itself. No read-only
/// mode is set, because `rsfbclient`, like `tiberius` before it in the sibling crate, has
/// no client-side concept of one: the account named in `config.connection_string` must
/// itself be provisioned with `SELECT`-only grants, a deployment requirement this
/// function cannot enforce (see `CLAUDE.md`'s Security requirement 4).
///
/// # Errors
///
/// [`Error::Connect`] if the connection string cannot be parsed, the socket cannot be
/// opened, or the login is refused. The connection string and, if present, its password
/// value are redacted from the message.
///
/// # Panics
///
/// Does not panic.
///
/// # Blocking
///
/// Synchronous: blocks the calling thread on network I/O. Call from a context that
/// expects that (a `spawn_blocking` task, a dedicated thread, or a caller with no
/// runtime of its own); [`open`] is the async wrapper for a Tokio caller.
pub fn open_blocking(config: &ConnectConfig) -> Result<FbConnection> {
    rsfbclient::builder_pure_rust()
        .from_string(&config.connection_string)
        .map_err(|e| connect_error(&e, &config.connection_string))?
        .connect()
        .map_err(|e| connect_error(&e, &config.connection_string))
}

/// Opens a connection described by `config`, without blocking the calling Tokio task.
///
/// Runs [`open_blocking`] on Tokio's blocking-thread pool via
/// `tokio::task::spawn_blocking`, and races it against `config.login_timeout_sec` if
/// set. See [`ConnectConfig::login_timeout_sec`] for exactly what that timeout does and
/// does not cancel.
///
/// # Errors
///
/// [`Error::Connect`] for everything [`open_blocking`] can produce, plus a timeout
/// message if `login_timeout_sec` elapses first. [`Error::Io`] if the blocking task
/// itself panicked, which would be a defect in `rsfbclient` or this module rather than a
/// connectivity problem.
///
/// # Panics
///
/// Does not panic.
///
/// # Blocking
///
/// Async; must run on a Tokio runtime that has blocking threads available (the default
/// for `tokio::runtime::Builder::new_multi_thread`, which is what `crate::pipeline`
/// always builds).
pub async fn open(config: &ConnectConfig) -> Result<FbConnection> {
    let owned = config.clone();
    let connect = async {
        tokio::task::spawn_blocking(move || open_blocking(&owned))
            .await
            .map_err(|e| Error::Io {
                message: format!("the connection task did not complete: {e}"),
            })?
    };

    match config.login_timeout_sec {
        Some(secs) => tokio::time::timeout(std::time::Duration::from_secs(secs), connect)
            .await
            .map_err(|_| Error::Connect {
                message: format!(
                    "timed out after {secs}s waiting for the connection (the underlying \
                     attempt may still be running; see ConnectConfig::login_timeout_sec)"
                ),
            })?,
        None => connect.await,
    }
}

/// Converts an [`FbError`] into this crate's [`Error::Connect`], redacting
/// `connection_string` (and, separately, its password value alone) from the message.
///
/// `pub(crate)` so every module that talks to `rsfbclient` directly goes through the same
/// redaction rather than each hand-rolling it.
///
/// # Panics
///
/// Does not panic.
pub(crate) fn connect_error(e: &FbError, connection_string: &str) -> Error {
    Error::Connect {
        message: redact(&e.to_string(), connection_string),
    }
}

/// Converts an [`FbError`] raised while running a query into [`Error::Query`], with the
/// same redaction applied.
///
/// Separate from [`connect_error`] only so a caller can tell a failed connection from a
/// failed statement; both redact identically, because a query error can quote the
/// session's own connection details just as a login error can.
///
/// # Panics
///
/// Does not panic.
pub(crate) fn query_error(e: &FbError, connection_string: &str) -> Error {
    Error::Query {
        message: redact(&e.to_string(), connection_string),
    }
}

/// Removes `connection_string`, and its password value if one can be read from it, from
/// `message`.
///
/// Two passes, deliberately, the same shape as `tiberiusdelta::connect::redact`: an
/// error that echoes the whole connection string is caught by the first, and one that
/// only echoes the password value on its own (for example inside a differently-worded
/// diagnostic that never repeats the full string) is still caught by the second. Neither
/// pass assumes the other fired.
///
/// # Panics
///
/// Does not panic.
pub(crate) fn redact(message: &str, connection_string: &str) -> String {
    let mut out = message.replace(connection_string, "[connection string redacted]");
    if let Some(password) = extract_password(connection_string)
        && !password.is_empty()
    {
        out = out.replace(&password, "[redacted]");
        let decoded = percent_decode(&password);
        if decoded != password {
            out = out.replace(&decoded, "[redacted]");
        }
    }
    out
}

/// Reads the password out of a `firebird://user:password@host:port/db` connection URL.
///
/// Matches the userinfo syntax `rsfbclient::connection::conn_string::parse` itself
/// accepts (`scheme://user:pass@...`), rather than a general-purpose URL parser: the
/// only thing this function needs to get right is finding the same password that
/// module would, so both agree on what must be redacted. Returns `None` if the string
/// has no `://`, no `@` after it (so no userinfo section at all), or no `:` inside the
/// userinfo section (a bare username, no password).
///
/// Redacts the password as it appears in the connection string, percent-encoding
/// included: a percent-encoded value (`abc%40def`) is checked against the message
/// verbatim first, then against its decoded form (`abc@def`), since `rsfbclient` itself
/// decodes the password before it could ever appear in one of its own error messages.
///
/// # Panics
///
/// Does not panic.
fn extract_password(connection_string: &str) -> Option<String> {
    let after_scheme = connection_string.split_once("://")?.1;
    let at = after_scheme.find('@')?;
    let userinfo = &after_scheme[..at];
    let (_, password) = userinfo.split_once(':')?;
    if password.is_empty() {
        None
    } else {
        Some(password.to_string())
    }
}

/// Decodes `%XX` percent-escapes in `s` as UTF-8 bytes, leaving anything that is not a
/// valid escape or does not decode as UTF-8 untouched.
///
/// A minimal, best-effort decoder rather than a dependency on the `percent-encoding`
/// crate `rsfbclient` itself already pulls in transitively: this function exists only so
/// [`redact`] can also catch a password that was percent-encoded in the connection
/// string but appears decoded in an error message, which is a defence-in-depth
/// improvement, not the primary redaction path (the whole-string replace already
/// handles the common case of an error simply echoing the input back verbatim).
///
/// # Panics
///
/// Does not panic.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok();
            if let Some(byte) = hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                out.push(byte);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap_or_else(|_| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_password_from_a_firebird_url() {
        assert_eq!(
            extract_password("firebird://sysdba:hunter2@localhost:3050/db.fdb"),
            Some("hunter2".to_string())
        );
    }

    #[test]
    fn a_bare_username_with_no_password_yields_none() {
        assert_eq!(
            extract_password("firebird://sysdba@localhost:3050/db.fdb"),
            None
        );
    }

    #[test]
    fn no_userinfo_at_all_yields_none() {
        assert_eq!(extract_password("firebird://localhost:3050/db.fdb"), None);
    }

    #[test]
    fn an_embedded_connection_string_with_no_host_yields_none() {
        assert_eq!(extract_password("firebird:///srv/db/db.fdb"), None);
    }

    #[test]
    fn redact_strips_the_whole_connection_string_when_echoed_verbatim() {
        let cs = "firebird://sysdba:hunter2@localhost:3050/db.fdb";
        let message = format!("Error on parse the string: invalid URL '{cs}'");
        let redacted = redact(&message, cs);
        assert!(!redacted.contains("hunter2"));
        assert!(!redacted.contains(cs));
    }

    /// An error that never echoes the full string but still leaks the bare password
    /// value on its own must still be caught.
    #[test]
    fn redact_strips_a_bare_password_value_even_without_the_full_string() {
        let cs = "firebird://sysdba:hunter2@localhost:3050/db.fdb";
        let message = "Login failed for user 'sysdba' with password hunter2";
        let redacted = redact(message, cs);
        assert!(!redacted.contains("hunter2"));
    }

    /// The percent-decoded form must also be caught, since `rsfbclient` decodes the
    /// password before it could reach one of its own error messages.
    #[test]
    fn redact_strips_a_percent_encoded_password_in_its_decoded_form() {
        let cs = "firebird://sysdba:abc%40def@localhost:3050/db.fdb";
        let message = "Login failed for user 'sysdba' with password abc@def";
        assert_eq!(percent_decode("abc%40def"), "abc@def");
        let redacted = redact(message, cs);
        assert!(!redacted.contains("abc@def"));
    }

    #[test]
    fn redact_is_a_no_op_when_nothing_secret_appears() {
        let cs = "firebird://sysdba:hunter2@localhost:3050/db.fdb";
        let message = "the server was unreachable";
        assert_eq!(redact(message, cs), message);
    }

    #[test]
    fn connect_config_debug_never_prints_the_connection_string() {
        let config = ConnectConfig {
            connection_string: "firebird://sysdba:hunter2@localhost:3050/db.fdb".to_string(),
            login_timeout_sec: Some(5),
        };
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("hunter2"));
        assert!(!rendered.contains("db.fdb"));
    }

    /// An unparseable connection string must fail through the redacting path, not carry
    /// the string (and its password) out in `rsfbclient`'s own message.
    #[tokio::test]
    async fn a_rejected_connection_string_is_reported_without_its_password() {
        let config = ConnectConfig {
            connection_string: "not a firebird url;pass=hunter2".to_string(),
            login_timeout_sec: Some(1),
        };
        // Not `.unwrap_err()`: that requires the `Ok` type (`FbConnection`) to be
        // `Debug`, which it deliberately is not (see `crate::connect`'s own type; a
        // live session has nothing safe to print).
        let err = match open(&config).await {
            Err(e) => e,
            Ok(_) => panic!("an unparseable connection string must not succeed"),
        };
        assert!(matches!(err, Error::Connect { .. }));
        assert!(!err.to_string().contains("hunter2"));
    }
}
