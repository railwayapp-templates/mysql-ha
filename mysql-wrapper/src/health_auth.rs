//! HTTP Basic auth for the health server's mutating routes.
//!
//! `HEALTH_API_PASSWORD` set → `POST /switchover` requires
//! `Authorization: Basic base64(HEALTH_API_USERNAME:HEALTH_API_PASSWORD)`
//! (username default `railway`); anything else answers 401 with a
//! `WWW-Authenticate` challenge. Reads stay open regardless: HAProxy's
//! routing probe and every peer's bootstrap guard read `/role`, `/health`,
//! `/gr/state` and `/pitr` without a credential, and nothing they learn
//! there can change the group. Unset → the route is open, exactly as
//! before, so a cluster picks enforcement up one variable at a time.
//!
//! The layer is scoped to the mutating sub-router (see
//! `health_server::run_health_server`), never to the whole app: a credential
//! on the read routes would take HAProxy's probe down with it.

use axum::{
    extract::{Request, State},
    http::{header, HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use std::sync::Arc;
use tracing::warn;

pub const USERNAME_ENV: &str = "HEALTH_API_USERNAME";
pub const PASSWORD_ENV: &str = "HEALTH_API_PASSWORD";
pub const DEFAULT_USERNAME: &str = "railway";
const CHALLENGE: &str = "Basic realm=\"railway-ha\"";

/// The credential a mutating request must present.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Credential {
    pub username: String,
    pub password: String,
}

/// What the middleware carries: `None` leaves the route open.
pub type Guard = Option<Arc<Credential>>;

impl Credential {
    /// The credential the environment configures. A blank password (unset,
    /// empty, whitespace) means no enforcement; a blank username falls back
    /// to [`DEFAULT_USERNAME`].
    pub fn from_env_values(username: Option<&str>, password: Option<&str>) -> Option<Self> {
        let password = password.map(str::trim).filter(|p| !p.is_empty())?;
        let username = username
            .map(str::trim)
            .filter(|u| !u.is_empty())
            .unwrap_or(DEFAULT_USERNAME);
        Some(Self {
            username: username.to_string(),
            password: password.to_string(),
        })
    }

    /// Whether an `Authorization` header carries this credential. Both
    /// halves are always compared, in constant time, so a wrong username
    /// costs the same as a wrong password.
    pub fn accepts(&self, header: Option<&HeaderValue>) -> bool {
        let Some((username, password)) = header.and_then(parse_basic) else {
            return false;
        };
        let user_ok = ct_eq(username.as_bytes(), self.username.as_bytes());
        let pass_ok = ct_eq(password.as_bytes(), self.password.as_bytes());
        user_ok & pass_ok
    }
}

/// `Authorization: Basic <base64(user:pass)>` → `(user, pass)`. The scheme
/// is case-insensitive per RFC 7235; the password may itself contain `:`.
fn parse_basic(header: &HeaderValue) -> Option<(String, String)> {
    let value = header.to_str().ok()?;
    let (scheme, encoded) = value.trim().split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("basic") {
        return None;
    }
    let decoded = BASE64.decode(encoded.trim()).ok()?;
    let decoded = String::from_utf8(decoded).ok()?;
    let (user, pass) = decoded.split_once(':')?;
    Some((user.to_string(), pass.to_string()))
}

/// Constant-time byte equality: every position of the longer input is
/// visited and the length difference is folded into the same accumulator,
/// so neither a length mismatch nor an early differing byte returns sooner.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff = a.len() ^ b.len();
    for i in 0..a.len().max(b.len()) {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        diff |= usize::from(x ^ y);
    }
    diff == 0
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(
            header::WWW_AUTHENTICATE,
            HeaderValue::from_static(CHALLENGE),
        )],
        "unauthorized",
    )
        .into_response()
}

/// axum middleware for the mutating sub-router. Passes every request
/// through when no credential is configured; otherwise admits only requests
/// carrying it. The refused request is logged by method and path — never by
/// what it presented.
pub async fn require_credential(State(guard): State<Guard>, req: Request, next: Next) -> Response {
    let Some(credential) = guard.as_deref() else {
        return next.run(req).await;
    };
    if credential.accepts(req.headers().get(header::AUTHORIZATION)) {
        return next.run(req).await;
    }
    warn!(
        method = %req.method(),
        path = req.uri().path(),
        "refused a request to a mutating route without a valid credential"
    );
    unauthorized()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::{Method, Request},
        middleware::from_fn_with_state,
        routing::{get, post},
        Router,
    };
    use tower::ServiceExt;

    fn cred() -> Credential {
        Credential {
            username: "railway".into(),
            password: "s3cr3t:with:colons".into(),
        }
    }

    fn basic(user: &str, pass: &str) -> HeaderValue {
        HeaderValue::from_str(&format!(
            "Basic {}",
            BASE64.encode(format!("{user}:{pass}"))
        ))
        .unwrap()
    }

    #[test]
    fn blank_password_means_no_credential() {
        assert_eq!(Credential::from_env_values(None, None), None);
        assert_eq!(Credential::from_env_values(Some("u"), Some("")), None);
        assert_eq!(Credential::from_env_values(Some("u"), Some("   ")), None);
    }

    #[test]
    fn password_is_trimmed_and_username_defaults() {
        let c = Credential::from_env_values(None, Some("  pw \n")).unwrap();
        assert_eq!(c.username, DEFAULT_USERNAME);
        assert_eq!(c.password, "pw");
        let c = Credential::from_env_values(Some("  "), Some("pw")).unwrap();
        assert_eq!(c.username, DEFAULT_USERNAME);
        let c = Credential::from_env_values(Some(" ops "), Some("pw")).unwrap();
        assert_eq!(c.username, "ops");
    }

    #[test]
    fn header_missing_is_refused() {
        assert!(!cred().accepts(None));
    }

    #[test]
    fn non_basic_scheme_is_refused() {
        let token = BASE64.encode("railway:s3cr3t:with:colons");
        let bearer = HeaderValue::from_str(&format!("Bearer {token}")).unwrap();
        assert!(!cred().accepts(Some(&bearer)));
        // A bare token with no scheme at all is not Basic either.
        assert!(!cred().accepts(Some(&HeaderValue::from_str(&token).unwrap())));
    }

    #[test]
    fn bad_base64_is_refused() {
        let h = HeaderValue::from_static("Basic !!!not-base64!!!");
        assert!(!cred().accepts(Some(&h)));
        // Valid base64 that is not `user:pass` is refused too.
        let no_colon =
            HeaderValue::from_str(&format!("Basic {}", BASE64.encode("railway"))).unwrap();
        assert!(!cred().accepts(Some(&no_colon)));
    }

    #[test]
    fn wrong_username_is_refused() {
        assert!(!cred().accepts(Some(&basic("root", "s3cr3t:with:colons"))));
    }

    #[test]
    fn wrong_password_is_refused() {
        assert!(!cred().accepts(Some(&basic("railway", "s3cr3t"))));
        assert!(!cred().accepts(Some(&basic("railway", "s3cr3t:with:colons "))));
        assert!(!cred().accepts(Some(&basic("railway", ""))));
    }

    #[test]
    fn correct_credential_is_accepted() {
        assert!(cred().accepts(Some(&basic("railway", "s3cr3t:with:colons"))));
    }

    #[test]
    fn scheme_is_case_insensitive() {
        let token = BASE64.encode("railway:s3cr3t:with:colons");
        for scheme in ["basic", "BASIC", "bAsIc"] {
            let h = HeaderValue::from_str(&format!("{scheme} {token}")).unwrap();
            assert!(cred().accepts(Some(&h)), "{scheme}");
        }
    }

    #[test]
    fn constant_time_compare_handles_lengths() {
        assert!(ct_eq(b"", b""));
        assert!(ct_eq(b"abc", b"abc"));
        assert!(!ct_eq(b"abc", b"abd"));
        assert!(!ct_eq(b"abc", b"ab"));
        assert!(!ct_eq(b"ab", b"abc"));
        // A prefix padded with the zero byte the comparison substitutes for
        // a missing position must still differ (the length fold catches it).
        assert!(!ct_eq(b"ab\0", b"ab"));
    }

    /// The router shape `health_server` uses: open reads merged with a
    /// mutating sub-router that alone carries the layer.
    fn app(guard: Guard) -> Router {
        let mutating = Router::new()
            .route(
                "/switchover",
                post(|| async { (StatusCode::OK, "switched") }),
            )
            .route_layer(from_fn_with_state(guard, require_credential));
        Router::new()
            .route("/health", get(|| async { (StatusCode::OK, "ok") }))
            .route(
                "/role",
                get(|| async { (StatusCode::SERVICE_UNAVAILABLE, "not primary") }),
            )
            .merge(mutating)
    }

    fn req(method: Method, path: &str, auth: Option<HeaderValue>) -> Request<Body> {
        let mut builder = Request::builder().method(method).uri(path);
        if let Some(h) = auth {
            builder = builder.header(header::AUTHORIZATION, h);
        }
        builder.body(Body::empty()).unwrap()
    }

    async fn body_text(resp: Response) -> String {
        let bytes = to_bytes(resp.into_body(), 1024).await.unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn no_credential_configured_leaves_the_route_open() {
        let resp = app(None)
            .oneshot(req(Method::POST, "/switchover", None))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(body_text(resp).await, "switched");
    }

    #[tokio::test]
    async fn mutating_route_is_gated_and_answers_a_challenge() {
        let guard: Guard = Some(Arc::new(cred()));

        let resp = app(guard.clone())
            .oneshot(req(Method::POST, "/switchover", None))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            resp.headers().get(header::WWW_AUTHENTICATE).unwrap(),
            &HeaderValue::from_static(CHALLENGE)
        );
        assert_eq!(body_text(resp).await, "unauthorized");

        let resp = app(guard.clone())
            .oneshot(req(
                Method::POST,
                "/switchover",
                Some(basic("railway", "wrong")),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

        let resp = app(guard)
            .oneshot(req(
                Method::POST,
                "/switchover",
                Some(basic("railway", "s3cr3t:with:colons")),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(body_text(resp).await, "switched");
    }

    #[tokio::test]
    async fn read_routes_bypass_the_layer() {
        let guard: Guard = Some(Arc::new(cred()));
        let resp = app(guard.clone())
            .oneshot(req(Method::GET, "/health", None))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        // The read handler's own verdict comes through untouched — 503 here
        // is the handler speaking, not the guard.
        let resp = app(guard)
            .oneshot(req(Method::GET, "/role", None))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body_text(resp).await, "not primary");
    }
}
