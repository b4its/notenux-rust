use crate::{
    cache::AppCache,
    models::{AuthError, AuthUser, SessionRow, TokenRow},
};
use axum::{
    extract::{FromRef, FromRequestParts},
    http::{header, request::Parts},
};
use sha2::{Digest, Sha256};
use sqlx::MySqlPool;

pub fn hash_token(plain: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(plain.as_bytes());
    hex::encode(hasher.finalize())
}

/// Axum Extractor for AuthUser: enables handlers to simply declare `user: AuthUser`
impl<S> FromRequestParts<S> for AuthUser
where
    S: Send + Sync,
    crate::AppState: FromRef<S>,
{
    type Rejection = AuthError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let app_state = crate::AppState::from_ref(state);
        authenticate(parts, &app_state.pool, &app_state.cache).await
    }
}

/// Validates Laravel Sanctum token or session cookie.
/// Caches successful token validations in Moka for 10 minutes to eliminate DB queries on chunk fetches.
pub async fn authenticate(
    parts: &Parts,
    pool: &MySqlPool,
    cache: &AppCache,
) -> Result<AuthUser, AuthError> {
    // 1. Check Authorization Bearer header: {id}|{plain_text_token}
    if let Some(auth_val) = parts.headers.get(header::AUTHORIZATION) {
        if let Ok(auth_str) = auth_val.to_str() {
            if let Some(token) = auth_str.strip_prefix("Bearer ").map(|s| s.trim()) {
                if !token.is_empty() {
                    return validate_sanctum_token(token, pool, cache).await;
                }
            }
        }
    }

    // 2. Fallback: Session Cookie (for web session compatibility)
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

    // 3. Fallback: Developer Header (for testing environments)
    if let Some(dev_user_header) = parts.headers.get("x-dev-user-id") {
        if let Ok(s) = dev_user_header.to_str() {
            if let Ok(uid) = s.parse::<u64>() {
                return Ok(AuthUser { id: uid });
            }
        }
    }

    Err(AuthError::MissingCredentials)
}

/// Core Sanctum token validation:
/// - Extracts {id} and {plain_text_token}
/// - Computes SHA-256 hash
/// - Queries MySQL personal_access_tokens
/// - Caches valid user_id in Moka for 10 minutes
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
    let cache_key = match token_id_opt {
        Some(tid) => format!("sanctum:{}:{}", tid, hashed_secret),
        None => format!("sanctum:plain:{}", hashed_secret),
    };

    // Check 10-minute Moka cache first
    if let Some(cached_user_id) = cache.get_cached_user(&cache_key).await {
        return Ok(AuthUser { id: cached_user_id });
    }

    // Query personal_access_tokens from database
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
            tracing::error!("DB error verifying Sanctum token: {:?}", e);
            return Err(AuthError::DatabaseError);
        }
    };

    // Verify token hash
    if row.token != hashed_secret {
        return Err(AuthError::InvalidToken);
    }

    // Check expiration timestamp
    if let Some(expires_at) = row.expires_at {
        if expires_at <= chrono::Utc::now() {
            return Err(AuthError::InvalidToken);
        }
    }

    let user_id = row.tokenable_id;

    // Cache valid user in Moka (10 minute TTL)
    cache.cache_user(cache_key, user_id).await;

    Ok(AuthUser { id: user_id })
}

/// Fallback session validator
async fn validate_session(
    session_id: &str,
    pool: &MySqlPool,
    cache: &AppCache,
) -> Result<AuthUser, AuthError> {
    let cache_key = format!("session:{}", session_id);
    if let Some(cached_uid) = cache.get_cached_user(&cache_key).await {
        return Ok(AuthUser { id: cached_uid });
    }

    let now_ts = chrono::Utc::now().timestamp() as i32;
    // Session valid if last activity within 7 days
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
                cache.cache_user(cache_key, uid).await;
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
