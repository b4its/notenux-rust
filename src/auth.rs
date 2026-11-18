use crate::{
    cache::AppCache,
    models::{AuthError, AuthUser, TokenRow},
};
use axum::{
    extract::{FromRef, FromRequestParts},
    http::{header, request::Parts},
};
use sha2::{Digest, Sha256};
use sqlx::MySqlPool;

pub fn hash_token(plain: &str) -> String {
    hex::encode(Sha256::digest(plain.as_bytes()))
}

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

pub async fn authenticate(
    parts: &Parts,
    pool: &MySqlPool,
    cache: &AppCache,
) -> Result<AuthUser, AuthError> {
    let raw = parts
        .headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or(AuthError::MissingCredentials)?;
    let (id, secret) = raw.split_once('|').ok_or(AuthError::InvalidToken)?;
    let id = id
        .parse::<u64>()
        .ok()
        .filter(|id| *id > 0)
        .ok_or(AuthError::InvalidToken)?;
    if secret.is_empty() || secret.contains('|') {
        return Err(AuthError::InvalidToken);
    }
    let hash = hash_token(secret);
    let key = format!("{id}:{hash}");
    if let Some(user) = cache.get_cached_user(&key).await {
        return Ok(AuthUser { id: user });
    }
    let row = sqlx::query_as::<_, TokenRow>(
        "SELECT tokenable_id, token, expires_at FROM personal_access_tokens WHERE id = ? AND token = ? AND tokenable_type = ?"
    ).bind(id).bind(&hash).bind("App\\Models\\User").fetch_optional(pool).await.map_err(|e| {
        tracing::error!("DB error verifying Sanctum token: {e}"); AuthError::DatabaseError
    })?.ok_or(AuthError::InvalidToken)?;
    if row
        .expires_at
        .is_some_and(|expiry| expiry <= chrono::Utc::now())
    {
        return Err(AuthError::InvalidToken);
    }
    cache
        .cache_user(key, row.tokenable_id, row.expires_at)
        .await;
    Ok(AuthUser {
        id: row.tokenable_id,
    })
}
