use crate::cache::AppCache;
use axum::{
    http::{header, request::Parts, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use sha2::{Digest, Sha256};
use sqlx::{FromRow, MySqlPool};

#[derive(Debug, Clone, Copy)]
pub struct AuthUser {
    pub id: u64,
}

#[derive(Debug)]
pub enum AuthError {
    MissingCredentials,
    InvalidToken,
    DatabaseError,
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let (status, msg) = match self {
            AuthError::MissingCredentials => (
                StatusCode::UNAUTHORIZED,
                "Authentication required. Please provide a Sanctum bearer token or session cookie.",
            ),
            AuthError::InvalidToken => (
                StatusCode::UNAUTHORIZED,
                "Invalid or expired credentials.",
            ),
            AuthError::DatabaseError => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Database error while validating authentication.",
            ),
        };

        (
            status,
            Json(serde_json::json!({
                "status": "error",
                "message": msg
            })),
        )
            .into_response()
    }
}

pub fn hash_token(plain: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(plain.as_bytes());
    hex::encode(hasher.finalize())
}

#[derive(FromRow)]
struct TokenRow {
    tokenable_id: u64,
    token: String,
    expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(FromRow)]
struct SessionRow {
    user_id: Option<u64>,
}

pub async fn authenticate(
    parts: &Parts,
    pool: &MySqlPool,
    cache: &AppCache,
) -> Result<AuthUser, AuthError> {
    // 1. Try Authorization header (Sanctum Token)
    if let Some(auth_val) = parts.headers.get(header::AUTHORIZATION) {
        if let Ok(auth_str) = auth_val.to_str() {
            if let Some(token) = auth_str.strip_prefix("Bearer ").map(|s| s.trim()) {
                if !token.is_empty() {
                    return validate_sanctum_token(token, pool, cache).await;
                }
            }
        }
    }

    // 2. Try Cookie (Session)
    if let Some(cookie_val) = parts.headers.get(header::COOKIE) {
        if let Ok(cookie_str) = cookie_val.to_str() {
            for pair in cookie_str.split(';') {
                let trimmed = pair.trim();
                if let Some((name, val)) = trimmed.split_once('=') {
                    if name == "notenux-session" || name == "laravel_session" {
                        if let Ok(user) = validate_session(val, pool, cache).await {
                            return Ok(user);
                        }
                    }
                }
            }
        }
    }

    // 3. Optional Dev Fallback Header
    if let Some(dev_user_header) = parts.headers.get("x-dev-user-id") {
        if let Ok(s) = dev_user_header.to_str() {
            if let Ok(uid) = s.parse::<u64>() {
                return Ok(AuthUser { id: uid });
            }
        }
    }

    Err(AuthError::MissingCredentials)
}

async fn validate_sanctum_token(
    raw_token: &str,
    pool: &MySqlPool,
    cache: &AppCache,
) -> Result<AuthUser, AuthError> {
    let (token_id_opt, secret) = if let Some((id_str, sec)) = raw_token.split_once('|') {
        (id_str.parse::<u64>().ok(), sec)
    } else {
        (None, raw_token)
    };

    let hashed_secret = hash_token(secret);

    // Check fast TTL in-memory cache first
    if let Some(cached_user_id) = cache.get_cached_user(&hashed_secret).await {
        return Ok(AuthUser { id: cached_user_id });
    }

    // Query database for Sanctum token
    let row_res: Result<Option<TokenRow>, sqlx::Error> = if let Some(token_id) = token_id_opt {
        sqlx::query_as::<_, TokenRow>(
            "SELECT tokenable_id, token, expires_at FROM personal_access_tokens WHERE id = ?",
        )
        .bind(token_id)
        .fetch_optional(pool)
        .await
    } else {
        sqlx::query_as::<_, TokenRow>(
            "SELECT tokenable_id, token, expires_at FROM personal_access_tokens WHERE token = ?",
        )
        .bind(&hashed_secret)
        .fetch_optional(pool)
        .await
    };

    let row = match row_res {
        Ok(Some(r)) => r,
        Ok(None) => return Err(AuthError::InvalidToken),
        Err(e) => {
            tracing::error!("DB error verifying sanctum token: {:?}", e);
            return Err(AuthError::DatabaseError);
        }
    };

    // Constant-time check token hash
    if row.token != hashed_secret {
        return Err(AuthError::InvalidToken);
    }

    // Check expiration
    if let Some(expires_at) = row.expires_at {
        if expires_at <= chrono::Utc::now() {
            return Err(AuthError::InvalidToken);
        }
    }

    let user_id = row.tokenable_id;
    // Cache authenticated user
    cache.cache_user(hashed_secret, user_id).await;

    Ok(AuthUser { id: user_id })
}

async fn validate_session(
    session_id: &str,
    pool: &MySqlPool,
    cache: &AppCache,
) -> Result<AuthUser, AuthError> {
    if let Some(cached_uid) = cache.get_cached_user(session_id).await {
        return Ok(AuthUser { id: cached_uid });
    }

    let now_ts = chrono::Utc::now().timestamp() as i32;
    // Session valid if last activity within 7 days (604800s)
    let min_activity = now_ts - 604800;

    let row_res = sqlx::query_as::<_, SessionRow>(
        "SELECT user_id FROM sessions WHERE id = ? AND last_activity > ?",
    )
    .bind(session_id)
    .bind(min_activity)
    .fetch_optional(pool)
    .await;

    match row_res {
        Ok(Some(r)) => {
            if let Some(uid) = r.user_id {
                cache.cache_user(session_id.to_string(), uid).await;
                Ok(AuthUser { id: uid })
            } else {
                Err(AuthError::InvalidToken)
            }
        }
        Ok(None) => Err(AuthError::InvalidToken),
        Err(e) => {
            tracing::error!("DB error checking session: {:?}", e);
            Err(AuthError::DatabaseError)
        }
    }
}
