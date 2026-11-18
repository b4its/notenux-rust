use crate::{
    models::{AuthUser, NoteSummary, NoteSummaryRow, SearchQuery},
    routes::ApiError,
    AppState,
};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use sqlx::MySql;

pub async fn search_notes(
    State(state): State<AppState>,
    user: AuthUser,
    Query(query): Query<SearchQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let search_term = query.q.as_deref().unwrap_or("").trim();
    if search_term.is_empty() {
        return Ok(Json(serde_json::json!({
            "status": "success",
            "data": Vec::<NoteSummary>::new()
        })));
    }

    let pattern = format!("%{}%", search_term);
    let mut query_builder = sqlx::QueryBuilder::<MySql>::new(
        r#"
        SELECT 
            n.id, n.user_id, n.folder_id, n.uuid, n.title, n.category,
            n.font_family, n.font_size, n.theme_color, n.word_count, n.char_count,
            n.is_pinned, n.is_favorite, n.level, n.xp, n.created_at, n.updated_at,
            (SELECT COUNT(*) FROM note_chunks c WHERE c.note_id = n.id) AS chunks_count
        FROM notes n
        WHERE n.user_id = 
        "#,
    );
    query_builder
        .push_bind(user.id)
        .push(" AND (n.title LIKE ")
        .push_bind(pattern.clone())
        .push(" OR n.content LIKE ")
        .push_bind(pattern.clone())
        .push(" OR EXISTS (SELECT 1 FROM note_chunks c WHERE c.note_id = n.id AND c.content LIKE ")
        .push_bind(pattern)
        .push(")) ORDER BY n.is_pinned DESC, n.updated_at DESC");

    let rows = query_builder
        .build_query_as::<NoteSummaryRow>()
        .fetch_all(&state.pool)
        .await
        .map_err(|e| {
            tracing::error!("Failed to search notes: {:?}", e);
            ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Database error during search".into(),
            )
        })?;

    let summaries: Vec<NoteSummary> = rows
        .into_iter()
        .map(|r| {
            let words = r.word_count;
            let mins = if words == 0 {
                1
            } else {
                (words as f64 / 180.0).ceil() as u32
            };
            NoteSummary {
                id: r.id,
                user_id: r.user_id,
                folder_id: r.folder_id,
                uuid: r.uuid,
                title: r.title,
                category: r.category,
                font_family: r.font_family,
                font_size: r.font_size,
                theme_color: r.theme_color,
                word_count: r.word_count,
                char_count: r.char_count,
                is_pinned: r.is_pinned,
                is_favorite: r.is_favorite,
                level: r.level,
                xp: r.xp,
                created_at: r.created_at,
                updated_at: r.updated_at,
                reading_time: format!("{} min read", mins),
                chunks_count: r.chunks_count.max(0) as u64,
            }
        })
        .collect();

    Ok(Json(serde_json::json!({
        "status": "success",
        "query": search_term,
        "count": summaries.len(),
        "data": summaries
    })))
}
