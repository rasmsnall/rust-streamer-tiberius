//! Opening a SQL Server connection, and keeping secrets out of everything that can fail.
//!
//! Async; must run on a Tokio runtime, the same as `crate::merge` and
//! `crate::checkpoint`. `tiberius` is runtime-agnostic and does not open its own socket:
//! this module opens a `tokio::net::TcpStream`, adapts it to the `futures` IO traits
//! `tiberius` expects, and hands it to [`tiberius::Client::connect`], which performs the
//! TDS login and, if the connection string asks for it, the TLS handshake.
//!
//! # Why this module exists, not just a call to `Client::connect`
//!
//! A connection failure's message can echo back the connection string it was given
//! verbatim, password included ("invalid connection string attribute", or similar).
//! [`Error::Connect`] must never carry that, because it reaches Python tracebacks and, if
//! a caller logs the exception, a log file. Every error this module produces is passed
//! through this module's own (private) redaction step first.

use std::fmt;

use tiberius::{Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::error::{Error, Result};

/// An open SQL Server session.
///
/// The concrete `tiberius::Client` this crate uses, named once here so the
/// `tokio_util::compat` wrapper does not have to be spelled out at every call site.
pub type SqlClient = Client<Compat<TcpStream>>;

/// Settings for opening one SQL Server connection.
///
/// Deliberately not `#[derive(Debug)]`: see the hand-written [`fmt::Debug`] impl below,
/// which redacts [`ConnectConfig::connection_string`] so an incidental `{:?}` (a panic
/// message, a debug log elsewhere in a caller's code) cannot leak it.
#[derive(Clone)]
pub struct ConnectConfig {
    /// The full ADO.NET-style connection string, for example
    /// `Server=tcp:host,1433;Database=db;User Id=svc;Password=...;TrustServerCertificate=true`.
    /// Carries the password; that is unavoidable; what this module guarantees is that it
    /// never escapes into an error message.
    ///
    /// Parsed by [`tiberius::Config::from_ado_string`].
    pub connection_string: String,
    /// Seconds allowed to establish the connection (TCP connect plus TDS login) before
    /// failing. `None` waits as long as the operating system's own TCP timeout, which is
    /// typically minutes.
    ///
    /// Enforced here with `tokio::time::timeout` rather than a driver setting:
    /// `tiberius` has no login-timeout option of its own.
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

/// Opens a connection described by `config`.
///
/// This issues no statement of any kind beyond the TDS login itself. No read-only mode
/// is set, because TDS has no client-side concept of one: the account named in
/// `config.connection_string` must itself be provisioned with `SELECT`-only grants, a
/// deployment requirement this function cannot enforce (see `CLAUDE.md`'s Security
/// requirement 4).
///
/// # Errors
///
/// [`Error::Connect`] if the connection string cannot be parsed, the socket cannot be
/// opened, the login is refused, or `login_timeout_sec` elapses first. The connection
/// string and, if present, its password value are redacted from the message.
///
/// # Panics
///
/// Does not panic.
///
/// # Blocking
///
/// Async; must run on a Tokio runtime.
pub async fn open(config: &ConnectConfig) -> Result<SqlClient> {
    let ado = Config::from_ado_string(&config.connection_string)
        .map_err(|e| connect_error(&e, &config.connection_string))?;

    let connect = async {
        let tcp = TcpStream::connect(ado.get_addr())
            .await
            .map_err(|e| Error::Connect {
                message: redact(&e.to_string(), &config.connection_string),
            })?;
        // Disables Nagle's algorithm. Every TDS round trip here is request/response, so
        // waiting to coalesce a follow-up write that is not coming adds latency per
        // batch for no benefit; tiberius's own documented example sets this too.
        tcp.set_nodelay(true).map_err(|e| Error::Connect {
            message: redact(&e.to_string(), &config.connection_string),
        })?;
        Client::connect(ado, tcp.compat_write())
            .await
            .map_err(|e| connect_error(&e, &config.connection_string))
    };

    match config.login_timeout_sec {
        Some(secs) => tokio::time::timeout(std::time::Duration::from_secs(secs), connect)
            .await
            .map_err(|_| Error::Connect {
                message: format!("timed out after {secs}s establishing the connection"),
            })?,
        None => connect.await,
    }
}

/// Converts a `tiberius::error::Error` into this crate's [`Error::Connect`], redacting
/// `connection_string` (and, separately, its password value alone) from the message.
///
/// `pub(crate)` so every module that talks to `tiberius` directly goes through the same
/// redaction rather than each hand-rolling it.
///
/// # Panics
///
/// Does not panic.
pub(crate) fn connect_error(e: &tiberius::error::Error, connection_string: &str) -> Error {
    Error::Connect {
        message: redact(&e.to_string(), connection_string),
    }
}

/// Converts a `tiberius::error::Error` raised while running a query into
/// [`Error::Query`], with the same redaction applied.
///
/// Separate from [`connect_error`] only so a caller can tell a failed connection from a
/// failed statement; both redact identically, because a query error can quote the
/// session's own connection details just as a login error can.
///
/// # Panics
///
/// Does not panic.
pub(crate) fn query_error(e: &tiberius::error::Error, connection_string: &str) -> Error {
    Error::Query {
        message: redact(&e.to_string(), connection_string),
    }
}

/// Removes `connection_string`, and its password value if one can be read from it, from
/// `message`.
///
/// Two passes, deliberately: an error that echoes the whole connection string is caught
/// by the first, and one that only echoes the password value on its own (for example
/// inside a differently-worded diagnostic that never repeats the full string) is still
/// caught by the second. Neither pass assumes the other fired.
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
    }
    out
}

/// Reads the `Password=` or `PWD=` value out of a connection string.
///
/// Connection strings are `;`-separated `key=value` pairs. Matching is case-insensitive
/// on the key, and tolerates the spacing ADO.NET allows (`User Id`, `Password `), since
/// both spellings and either casing are accepted by
/// [`tiberius::Config::from_ado_string`]. Returns `None` if neither key is present, or
/// the value is empty.
///
/// # Panics
///
/// Does not panic.
fn extract_password(connection_string: &str) -> Option<String> {
    for pair in connection_string.split(';') {
        let Some((key, value)) = pair.split_once('=') else {
            // A malformed segment is not a reason to stop looking: the password may
            // still be in a later, well-formed one.
            continue;
        };
        let key = key.trim();
        let value = value.trim();
        if (key.eq_ignore_ascii_case("pwd") || key.eq_ignore_ascii_case("password"))
            && !value.is_empty()
        {
            return Some(value.to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_password_regardless_of_key_spelling_or_case() {
        assert_eq!(
            extract_password("Server=x;Database=y;PWD=hunter2;"),
            Some("hunter2".to_string())
        );
        assert_eq!(
            extract_password("Server=x;Password=hunter2;Database=y"),
            Some("hunter2".to_string())
        );
        assert_eq!(
            extract_password("Server=x;pwd=hunter2"),
            Some("hunter2".to_string())
        );
    }

    /// The ODBC-era version stopped at the first segment without an `=`, which would
    /// have let a password through unredacted if anything preceded it in the string.
    /// ADO.NET strings routinely end with a trailing `;`, producing exactly such a
    /// segment.
    #[test]
    fn a_malformed_segment_does_not_abandon_the_search() {
        assert_eq!(
            extract_password("Integrated Security;Server=x;Password=hunter2;"),
            Some("hunter2".to_string())
        );
    }

    #[test]
    fn no_password_key_yields_none() {
        assert_eq!(extract_password("Server=x;Database=y;User Id=admin;"), None);
    }

    #[test]
    fn an_empty_password_value_yields_none() {
        assert_eq!(extract_password("Server=x;PWD=;"), None);
    }

    #[test]
    fn redact_strips_the_whole_connection_string_when_echoed_verbatim() {
        let cs = "Server=tcp:db,1433;Database=app;User Id=svc;Password=hunter2;";
        let message = format!("invalid connection string attribute: '{cs}'");
        let redacted = redact(&message, cs);
        assert!(!redacted.contains("hunter2"));
        assert!(!redacted.contains(cs));
    }

    /// An error that never echoes the full string but still leaks the bare password
    /// value on its own must still be caught.
    #[test]
    fn redact_strips_a_bare_password_value_even_without_the_full_string() {
        let cs = "Server=x;Database=y;Password=hunter2;";
        let message = "Login failed for user 'svc' with password hunter2";
        let redacted = redact(message, cs);
        assert!(!redacted.contains("hunter2"));
    }

    #[test]
    fn redact_is_a_no_op_when_nothing_secret_appears() {
        let cs = "Server=x;Database=y;Password=hunter2;";
        let message = "the server was unreachable";
        assert_eq!(redact(message, cs), message);
    }

    #[test]
    fn connect_config_debug_never_prints_the_connection_string() {
        let config = ConnectConfig {
            connection_string: "Server=x;Password=hunter2;".to_string(),
            login_timeout_sec: Some(5),
        };
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("hunter2"));
        assert!(!rendered.contains("Password"));
    }

    /// An unparseable connection string must fail through the redacting path, not carry
    /// the string (and its password) out in `tiberius`'s own message.
    #[tokio::test]
    async fn a_rejected_connection_string_is_reported_without_its_password() {
        let config = ConnectConfig {
            connection_string: "this is not a connection string;Password=hunter2".to_string(),
            login_timeout_sec: Some(1),
        };
        let err = open(&config).await.unwrap_err();
        assert!(matches!(err, Error::Connect { .. }));
        assert!(!err.to_string().contains("hunter2"));
    }
}
