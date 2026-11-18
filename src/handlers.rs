use crate::{
    auth::authenticate,
    cache::AppCache,
    chunker::{calculate_metrics, compute_checksum, detect_chunk_type, split_into_chunks},
    models::{
        ChunksQuery, CreateNotePayload, Note, NoteChunk, NoteSummary, NoteWithChunks,
        NotesListQuery, ReorderChunksPayload, UpdateChunkPayload, UpdateNotePayload,
    },
};
use axum::{
    extract::{Path, Query, State},
    http::{request::Parts, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use sqlx::{FromRow, MySql, MySqlPool};

#[derive(Clone)]
pub struct AppState {
    pub pool: MySqlPool,
    pub cache: AppCache,
}

pub struct ApiError(pub StatusCode, pub String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.0,
            Json(serde_json::json!({
                "status": "error",
                "message": self.1
            })),
        )
            .into_response()
    }
}

pub async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    let ping = sqlx::query("SELECT 1").execute(&state.pool).await.is_ok();
    Json(serde_json::json!({
        "status": if ping { "healthy" } else { "degraded" },
        "service": "be-notenux",
        "database_connected": ping,
        "engine": "rust-heavy-lifter",
        "cache": "moka-bounded-lru",
    }))
}

#[derive(FromRow)]
struct NoteSummaryRow {
    id: u64,
    user_id: u64,
    folder_id: Option<u64>,
    uuid: String,
    title: String,
    category: String,
    font_family: String,
    font_size: String,
    theme_color: String,
    word_count: u32,
    char_count: u32,
    is_pinned: bool,
    is_favorite: bool,
    level: u32,
    xp: u32,
    created_at: Option<chrono::DateTime<chrono::Utc>>,
    updated_at: Option<chrono::DateTime<chrono::Utc>>,
    chunks_count: i64,
}

pub async fn list_notes(
    State(state): State<AppState>,
    parts: Parts,
    Query(query): Query<NotesListQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let user = authenticate(&parts, &state.pool, &state.cache)
        .await
        .map_err(|e| ApiError(StatusCode::UNAUTHORIZED, format!("{:?}", e)))?;

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
    query_builder.push_bind(user.id);

    // Apply filters
    if let Some(ref cat) = query.category {
        if cat == "pinned" {
            query_builder.push(" AND n.is_pinned = 1");
        } else if cat == "favorites" {
            query_builder.push(" AND n.is_favorite = 1");
        } else if cat != "all" {
            query_builder.push(" AND n.category = ").push_bind(cat);
        }
    }

    if let Some(true) = query.pinned_only {
        query_builder.push(" AND n.is_pinned = 1");
    }

    if let Some(true) = query.favorites_only {
        query_builder.push(" AND n.is_favorite = 1");
    }

    if let Some(fid) = query.folder_id {
        query_builder.push(" AND n.folder_id = ").push_bind(fid);
    }

    if let Some(ref search) = query.q {
        let pattern = format!("%{}%", search);
        query_builder
            .push(" AND (n.title LIKE ")
            .push_bind(pattern.clone())
            .push(" OR n.content LIKE ")
            .push_bind(pattern.clone())
            .push(" OR EXISTS (SELECT 1 FROM note_chunks c WHERE c.note_id = n.id AND c.content LIKE ")
            .push_bind(pattern)
            .push("))");
    }

    // Sorting
    let sort = query.sort.as_deref().unwrap_or("updated_desc");
    match sort {
        "title_asc" => query_builder.push(" ORDER BY n.is_pinned DESC, n.title ASC"),
        "title_desc" => query_builder.push(" ORDER BY n.is_pinned DESC, n.title DESC"),
        "created_asc" => query_builder.push(" ORDER BY n.is_pinned DESC, n.created_at ASC"),
        "created_desc" => query_builder.push(" ORDER BY n.is_pinned DESC, n.created_at DESC"),
        "word_count_desc" => query_builder.push(" ORDER BY n.is_pinned DESC, n.word_count DESC"),
        "level_desc" => query_builder.push(" ORDER BY n.is_pinned DESC, n.level DESC"),
        "updated_asc" => query_builder.push(" ORDER BY n.is_pinned DESC, n.updated_at ASC"),
        _ => query_builder.push(" ORDER BY n.is_pinned DESC, n.updated_at DESC"),
    };

    let rows = query_builder
        .build_query_as::<NoteSummaryRow>()
        .fetch_all(&state.pool)
        .await
        .map_err(|e| {
            tracing::error!("Failed to fetch notes: {:?}", e);
            ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to retrieve notes".into(),
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
        "data": summaries
    })))
}

pub async fn get_note(
    State(state): State<AppState>,
    parts: Parts,
    Path(note_id): Path<u64>,
) -> Result<impl IntoResponse, ApiError> {
    let user = authenticate(&parts, &state.pool, &state.cache)
        .await
        .map_err(|e| ApiError(StatusCode::UNAUTHORIZED, format!("{:?}", e)))?;

    // Check bounded LRU cache
    if let Some(cached) = state.cache.get_note(user.id, note_id).await {
        return Ok(Json(serde_json::json!({
            "status": "success",
            "data": NoteWithChunks {
                reading_time: cached.note.reading_time(),
                note: cached.note.clone(),
                chunks: cached.chunks.clone(),
            }
        })));
    }

    // Query Note
    let note = sqlx::query_as::<_, Note>(
        "SELECT id, user_id, folder_id, uuid, title, category, content, font_family, font_size, theme_color, word_count, char_count, is_pinned, is_favorite, level, xp, created_at, updated_at FROM notes WHERE id = ? AND user_id = ?"
    )
    .bind(note_id)
    .bind(user.id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|e| {
        tracing::error!("Error fetching note {}: {:?}", note_id, e);
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Database error".into())
    })?
    .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "Note not found".into()))?;

    // Query Chunks
    let chunks = sqlx::query_as::<_, NoteChunk>(
        "SELECT id, note_id, chunk_uid, chunk_index, chunk_type, content, byte_size, checksum, metadata, created_at, updated_at FROM note_chunks WHERE note_id = ? ORDER BY chunk_index ASC"
    )
    .bind(note_id)
    .fetch_all(&state.pool)
    .await
    .map_err(|e| {
        tracing::error!("Error fetching chunks for note {}: {:?}", note_id, e);
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Database error".into())
    })?;

    // Store in LRU cache
    state
        .cache
        .insert_note(user.id, note.clone(), chunks.clone())
        .await;

    Ok(Json(serde_json::json!({
        "status": "success",
        "data": NoteWithChunks {
            reading_time: note.reading_time(),
            note,
            chunks,
        }
    })))
}

pub async fn get_chunks(
    State(state): State<AppState>,
    parts: Parts,
    Path(note_id): Path<u64>,
    Query(query): Query<ChunksQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let user = authenticate(&parts, &state.pool, &state.cache)
        .await
        .map_err(|e| ApiError(StatusCode::UNAUTHORIZED, format!("{:?}", e)))?;

    // Verify note ownership
    let (uuid,): (String,) = sqlx::query_as("SELECT uuid FROM notes WHERE id = ? AND user_id = ?")
        .bind(note_id)
        .bind(user.id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Database error".into()))?
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "Note not found".into()))?;

    // Check cache for all chunks first
    let all_chunks = if let Some(cached) = state.cache.get_note(user.id, note_id).await {
        cached.chunks.clone()
    } else {
        sqlx::query_as::<_, NoteChunk>(
            "SELECT id, note_id, chunk_uid, chunk_index, chunk_type, content, byte_size, checksum, metadata, created_at, updated_at FROM note_chunks WHERE note_id = ? ORDER BY chunk_index ASC"
        )
        .bind(note_id)
        .fetch_all(&state.pool)
        .await
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Failed to load chunks".into()))?
    };

    let total_chunks = all_chunks.len();
    let total_bytes: u32 = all_chunks.iter().map(|c| c.byte_size).sum();

    // Progressive / lazy chunk slicing
    if let Some(limit) = query.limit {
        let offset = query.offset.unwrap_or(0) as usize;
        let limit = limit as usize;

        let end = (offset + limit).min(total_chunks);
        let sliced = if offset < total_chunks {
            all_chunks[offset..end].to_vec()
        } else {
            Vec::new()
        };

        return Ok(Json(serde_json::json!({
            "status": "success",
            "note_id": note_id,
            "note_uuid": uuid,
            "total_chunks": total_chunks,
            "total_bytes": total_bytes,
            "offset": offset,
            "limit": limit,
            "has_more": end < total_chunks,
            "chunks": sliced,
        })));
    }

    // Default: full list
    Ok(Json(serde_json::json!({
        "status": "success",
        "note_id": note_id,
        "note_uuid": uuid,
        "total_chunks": total_chunks,
        "total_bytes": total_bytes,
        "chunks": all_chunks,
    })))
}

pub async fn verify_chunks(
    State(state): State<AppState>,
    parts: Parts,
    Path(note_id): Path<u64>,
) -> Result<impl IntoResponse, ApiError> {
    let user = authenticate(&parts, &state.pool, &state.cache)
        .await
        .map_err(|e| ApiError(StatusCode::UNAUTHORIZED, format!("{:?}", e)))?;

    let (uuid,): (String,) = sqlx::query_as("SELECT uuid FROM notes WHERE id = ? AND user_id = ?")
        .bind(note_id)
        .bind(user.id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Database error".into()))?
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "Note not found".into()))?;

    let chunks = sqlx::query_as::<_, NoteChunk>(
        "SELECT id, note_id, chunk_uid, chunk_index, chunk_type, content, byte_size, checksum, metadata, created_at, updated_at FROM note_chunks WHERE note_id = ? ORDER BY chunk_index ASC"
    )
    .bind(note_id)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Database error".into()))?;

    let total = chunks.len();
    let mut valid = 0;
    let mut corrupted = 0;
    let mut details = Vec::new();

    for chunk in &chunks {
        let expected_checksum = compute_checksum(&chunk.content);
        let expected_bytes = chunk.content.len() as u32;

        let checksum_matches = chunk.checksum.as_deref() == Some(&expected_checksum);
        let bytes_match = chunk.byte_size == expected_bytes;
        let is_chunk_valid = checksum_matches && bytes_match;

        if is_chunk_valid {
            valid += 1;
        } else {
            corrupted += 1;
        }

        details.push(serde_json::json!({
            "chunk_uid": chunk.chunk_uid,
            "chunk_index": chunk.chunk_index,
            "is_valid": is_chunk_valid,
            "checksum_matches": checksum_matches,
            "bytes_match": bytes_match,
        }));
    }

    let all_verified = total > 0 && corrupted == 0;
    let status_str = if all_verified {
        "verified"
    } else if total == 0 {
        "empty"
    } else {
        "corrupted"
    };

    Ok(Json(serde_json::json!({
        "status": "success",
        "note_id": note_id,
        "note_uuid": uuid,
        "data": {
            "status": status_str,
            "verified": all_verified,
            "is_valid": all_verified,
            "total_chunks": total,
            "valid_chunks": valid,
            "corrupted_chunks": corrupted,
            "details": details,
        }
    })))
}

pub async fn create_note(
    State(state): State<AppState>,
    parts: Parts,
    Json(payload): Json<CreateNotePayload>,
) -> Result<impl IntoResponse, ApiError> {
    let user = authenticate(&parts, &state.pool, &state.cache)
        .await
        .map_err(|e| ApiError(StatusCode::UNAUTHORIZED, format!("{:?}", e)))?;

    let raw_title = payload.title.as_deref().unwrap_or("Catatan Baru").trim();
    let title = if raw_title.is_empty() {
        "Catatan Baru"
    } else {
        raw_title
    };
    let category = payload.category.as_deref().unwrap_or("catatan").trim();
    let content = payload.content.unwrap_or_default();
    let font_family = payload.font_family.as_deref().unwrap_or("sans");
    let font_size = payload.font_size.as_deref().unwrap_or("16px");
    let theme_color = payload.theme_color.as_deref().unwrap_or("blue");
    let is_pinned = payload.is_pinned.unwrap_or(false);
    let is_favorite = payload.is_favorite.unwrap_or(false);
    let note_uuid = uuid::Uuid::new_v4().to_string();

    let metrics = calculate_metrics(&content);
    let chunk_payloads = split_into_chunks(&content);

    let mut tx = state.pool.begin().await.map_err(|e| {
        tracing::error!("Failed to begin transaction: {:?}", e);
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Transaction error".into())
    })?;

    let insert_res = sqlx::query(
        r#"
        INSERT INTO notes 
            (user_id, folder_id, uuid, title, category, content, font_family, font_size, theme_color, word_count, char_count, is_pinned, is_favorite, level, xp, created_at, updated_at)
        VALUES 
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
        "#,
    )
    .bind(user.id)
    .bind(payload.folder_id)
    .bind(&note_uuid)
    .bind(title)
    .bind(category)
    .bind(&content)
    .bind(font_family)
    .bind(font_size)
    .bind(theme_color)
    .bind(metrics.word_count)
    .bind(metrics.char_count)
    .bind(is_pinned)
    .bind(is_favorite)
    .bind(metrics.level)
    .bind(metrics.xp)
    .execute(&mut *tx)
    .await
    .map_err(|e| {
        tracing::error!("Failed to insert note: {:?}", e);
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Failed to create note".into())
    })?;

    let note_id = insert_res.last_insert_id();

    let mut created_chunks = Vec::new();
    for ch in chunk_payloads {
        let meta_json = ch.metadata.as_ref().map(|m| m.to_string());
        let chunk_res = sqlx::query(
            r#"
            INSERT INTO note_chunks 
                (note_id, chunk_uid, chunk_index, chunk_type, content, byte_size, checksum, metadata, created_at, updated_at)
            VALUES 
                (?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
            "#,
        )
        .bind(note_id)
        .bind(&ch.chunk_uid)
        .bind(ch.chunk_index)
        .bind(&ch.chunk_type)
        .bind(&ch.content)
        .bind(ch.byte_size)
        .bind(&ch.checksum)
        .bind(meta_json)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            tracing::error!("Failed to insert chunk: {:?}", e);
            ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Failed to create chunks".into())
        })?;

        created_chunks.push(NoteChunk {
            id: chunk_res.last_insert_id(),
            note_id,
            chunk_uid: ch.chunk_uid,
            chunk_index: ch.chunk_index,
            chunk_type: ch.chunk_type,
            content: ch.content,
            byte_size: ch.byte_size,
            checksum: Some(ch.checksum),
            metadata: ch.metadata,
            created_at: Some(chrono::Utc::now()),
            updated_at: Some(chrono::Utc::now()),
        });
    }

    tx.commit().await.map_err(|e| {
        tracing::error!("Commit failed: {:?}", e);
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Commit failed".into())
    })?;

    let created_note = Note {
        id: note_id,
        user_id: user.id,
        folder_id: payload.folder_id,
        uuid: note_uuid,
        title: title.to_string(),
        category: category.to_string(),
        content: Some(content),
        font_family: font_family.to_string(),
        font_size: font_size.to_string(),
        theme_color: theme_color.to_string(),
        word_count: metrics.word_count,
        char_count: metrics.char_count,
        is_pinned,
        is_favorite,
        level: metrics.level,
        xp: metrics.xp,
        created_at: Some(chrono::Utc::now()),
        updated_at: Some(chrono::Utc::now()),
    };

    state
        .cache
        .insert_note(user.id, created_note.clone(), created_chunks.clone())
        .await;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "status": "success",
            "message": "Scroll inscribed and chunked into memory matrix!",
            "data": NoteWithChunks {
                reading_time: created_note.reading_time(),
                note: created_note,
                chunks: created_chunks,
            }
        })),
    ))
}

pub async fn update_note(
    State(state): State<AppState>,
    parts: Parts,
    Path(note_id): Path<u64>,
    Json(payload): Json<UpdateNotePayload>,
) -> Result<impl IntoResponse, ApiError> {
    let user = authenticate(&parts, &state.pool, &state.cache)
        .await
        .map_err(|e| ApiError(StatusCode::UNAUTHORIZED, format!("{:?}", e)))?;

    let mut current_note = sqlx::query_as::<_, Note>(
        "SELECT id, user_id, folder_id, uuid, title, category, content, font_family, font_size, theme_color, word_count, char_count, is_pinned, is_favorite, level, xp, created_at, updated_at FROM notes WHERE id = ? AND user_id = ?"
    )
    .bind(note_id)
    .bind(user.id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Database error".into()))?
    .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "Note not found".into()))?;

    let mut tx = state.pool.begin().await.map_err(|_| {
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Transaction error".into())
    })?;

    let new_title = payload.title.as_deref().unwrap_or(&current_note.title);
    let new_category = payload
        .category
        .as_deref()
        .unwrap_or(&current_note.category);
    let new_folder_id = payload.folder_id.or(current_note.folder_id);
    let new_font_family = payload
        .font_family
        .as_deref()
        .unwrap_or(&current_note.font_family);
    let new_font_size = payload
        .font_size
        .as_deref()
        .unwrap_or(&current_note.font_size);
    let new_theme = payload
        .theme_color
        .as_deref()
        .unwrap_or(&current_note.theme_color);
    let new_pinned = payload.is_pinned.unwrap_or(current_note.is_pinned);
    let new_fav = payload.is_favorite.unwrap_or(current_note.is_favorite);

    let content_changed = payload.content.is_some();
    let new_content = payload
        .content
        .unwrap_or_else(|| current_note.content.clone().unwrap_or_default());

    let metrics = calculate_metrics(&new_content);

    sqlx::query(
        r#"
        UPDATE notes 
        SET 
            title = ?, category = ?, folder_id = ?, content = ?,
            font_family = ?, font_size = ?, theme_color = ?,
            word_count = ?, char_count = ?, is_pinned = ?, is_favorite = ?,
            level = ?, xp = ?, updated_at = NOW()
        WHERE id = ? AND user_id = ?
        "#,
    )
    .bind(new_title)
    .bind(new_category)
    .bind(new_folder_id)
    .bind(&new_content)
    .bind(new_font_family)
    .bind(new_font_size)
    .bind(new_theme)
    .bind(metrics.word_count)
    .bind(metrics.char_count)
    .bind(new_pinned)
    .bind(new_fav)
    .bind(metrics.level)
    .bind(metrics.xp)
    .bind(note_id)
    .bind(user.id)
    .execute(&mut *tx)
    .await
    .map_err(|e| {
        tracing::error!("Failed to update note: {:?}", e);
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Update failed".into())
    })?;

    let mut updated_chunks = Vec::new();
    if content_changed {
        sqlx::query("DELETE FROM note_chunks WHERE note_id = ?")
            .bind(note_id)
            .execute(&mut *tx)
            .await
            .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Chunk sync failed".into()))?;

        let drafts = split_into_chunks(&new_content);
        for ch in drafts {
            let meta_json = ch.metadata.as_ref().map(|m| m.to_string());
            let chunk_res = sqlx::query(
                r#"
                INSERT INTO note_chunks 
                    (note_id, chunk_uid, chunk_index, chunk_type, content, byte_size, checksum, metadata, created_at, updated_at)
                VALUES 
                    (?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
                "#,
            )
            .bind(note_id)
            .bind(&ch.chunk_uid)
            .bind(ch.chunk_index)
            .bind(&ch.chunk_type)
            .bind(&ch.content)
            .bind(ch.byte_size)
            .bind(&ch.checksum)
            .bind(meta_json)
            .execute(&mut *tx)
            .await
            .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Chunk insert failed".into()))?;

            updated_chunks.push(NoteChunk {
                id: chunk_res.last_insert_id(),
                note_id,
                chunk_uid: ch.chunk_uid,
                chunk_index: ch.chunk_index,
                chunk_type: ch.chunk_type,
                content: ch.content,
                byte_size: ch.byte_size,
                checksum: Some(ch.checksum),
                metadata: ch.metadata,
                created_at: Some(chrono::Utc::now()),
                updated_at: Some(chrono::Utc::now()),
            });
        }
    } else {
        updated_chunks = sqlx::query_as::<_, NoteChunk>(
            "SELECT id, note_id, chunk_uid, chunk_index, chunk_type, content, byte_size, checksum, metadata, created_at, updated_at FROM note_chunks WHERE note_id = ? ORDER BY chunk_index ASC"
        )
        .bind(note_id)
        .fetch_all(&mut *tx)
        .await
        .unwrap_or_default();
    }

    tx.commit().await.map_err(|_| {
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Commit failed".into())
    })?;

    current_note.title = new_title.to_string();
    current_note.category = new_category.to_string();
    current_note.folder_id = new_folder_id;
    current_note.content = Some(new_content);
    current_note.font_family = new_font_family.to_string();
    current_note.font_size = new_font_size.to_string();
    current_note.theme_color = new_theme.to_string();
    current_note.word_count = metrics.word_count;
    current_note.char_count = metrics.char_count;
    current_note.is_pinned = new_pinned;
    current_note.is_favorite = new_fav;
    current_note.level = metrics.level;
    current_note.xp = metrics.xp;
    current_note.updated_at = Some(chrono::Utc::now());

    state
        .cache
        .insert_note(user.id, current_note.clone(), updated_chunks.clone())
        .await;

    Ok(Json(serde_json::json!({
        "status": "success",
        "message": "Scroll updated and chunks harmonized!",
        "data": NoteWithChunks {
            reading_time: current_note.reading_time(),
            note: current_note,
            chunks: updated_chunks,
        }
    })))
}

pub async fn delete_note(
    State(state): State<AppState>,
    parts: Parts,
    Path(note_id): Path<u64>,
) -> Result<impl IntoResponse, ApiError> {
    let user = authenticate(&parts, &state.pool, &state.cache)
        .await
        .map_err(|e| ApiError(StatusCode::UNAUTHORIZED, format!("{:?}", e)))?;

    let mut tx = state.pool.begin().await.map_err(|_| {
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Transaction error".into())
    })?;

    sqlx::query("DELETE FROM note_chunks WHERE note_id = ?")
        .bind(note_id)
        .execute(&mut *tx)
        .await
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Delete chunks failed".into()))?;

    let res = sqlx::query("DELETE FROM notes WHERE id = ? AND user_id = ?")
        .bind(note_id)
        .bind(user.id)
        .execute(&mut *tx)
        .await
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Delete note failed".into()))?;

    if res.rows_affected() == 0 {
        return Err(ApiError(StatusCode::NOT_FOUND, "Note not found".into()));
    }

    tx.commit().await.map_err(|_| {
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Commit failed".into())
    })?;

    state.cache.invalidate_note(user.id, note_id).await;

    Ok(Json(serde_json::json!({
        "status": "success",
        "message": "Scroll banished to the digital void!"
    })))
}

pub async fn toggle_pin(
    State(state): State<AppState>,
    parts: Parts,
    Path(note_id): Path<u64>,
) -> Result<impl IntoResponse, ApiError> {
    let user = authenticate(&parts, &state.pool, &state.cache)
        .await
        .map_err(|e| ApiError(StatusCode::UNAUTHORIZED, format!("{:?}", e)))?;

    let res = sqlx::query(
        "UPDATE notes SET is_pinned = NOT is_pinned, updated_at = NOW() WHERE id = ? AND user_id = ?",
    )
    .bind(note_id)
    .bind(user.id)
    .execute(&state.pool)
    .await
    .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Database error".into()))?;

    if res.rows_affected() == 0 {
        return Err(ApiError(StatusCode::NOT_FOUND, "Note not found".into()));
    }

    state.cache.invalidate_note(user.id, note_id).await;

    let updated_pinned: (bool,) = sqlx::query_as("SELECT is_pinned FROM notes WHERE id = ?")
        .bind(note_id)
        .fetch_one(&state.pool)
        .await
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Database error".into()))?;

    Ok(Json(serde_json::json!({
        "status": "success",
        "is_pinned": updated_pinned.0,
        "message": if updated_pinned.0 { "Scroll pinned to top!" } else { "Scroll unpinned!" }
    })))
}

pub async fn toggle_favorite(
    State(state): State<AppState>,
    parts: Parts,
    Path(note_id): Path<u64>,
) -> Result<impl IntoResponse, ApiError> {
    let user = authenticate(&parts, &state.pool, &state.cache)
        .await
        .map_err(|e| ApiError(StatusCode::UNAUTHORIZED, format!("{:?}", e)))?;

    let res = sqlx::query(
        "UPDATE notes SET is_favorite = NOT is_favorite, updated_at = NOW() WHERE id = ? AND user_id = ?",
    )
    .bind(note_id)
    .bind(user.id)
    .execute(&state.pool)
    .await
    .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Database error".into()))?;

    if res.rows_affected() == 0 {
        return Err(ApiError(StatusCode::NOT_FOUND, "Note not found".into()));
    }

    state.cache.invalidate_note(user.id, note_id).await;

    let updated_fav: (bool,) = sqlx::query_as("SELECT is_favorite FROM notes WHERE id = ?")
        .bind(note_id)
        .fetch_one(&state.pool)
        .await
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Database error".into()))?;

    Ok(Json(serde_json::json!({
        "status": "success",
        "is_favorite": updated_fav.0,
        "message": if updated_fav.0 { "Scroll favorited!" } else { "Scroll unfavorited!" }
    })))
}

pub async fn duplicate_note(
    State(state): State<AppState>,
    parts: Parts,
    Path(note_id): Path<u64>,
) -> Result<impl IntoResponse, ApiError> {
    let user = authenticate(&parts, &state.pool, &state.cache)
        .await
        .map_err(|e| ApiError(StatusCode::UNAUTHORIZED, format!("{:?}", e)))?;

    // 1. Fetch source note
    let source_note = sqlx::query_as::<_, Note>(
        "SELECT id, user_id, folder_id, uuid, title, category, content, font_family, font_size, theme_color, word_count, char_count, is_pinned, is_favorite, level, xp, created_at, updated_at FROM notes WHERE id = ? AND user_id = ?"
    )
    .bind(note_id)
    .bind(user.id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Database error".into()))?
    .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "Note not found".into()))?;

    // 2. Fetch source chunks
    let source_chunks = sqlx::query_as::<_, NoteChunk>(
        "SELECT id, note_id, chunk_uid, chunk_index, chunk_type, content, byte_size, checksum, metadata, created_at, updated_at FROM note_chunks WHERE note_id = ? ORDER BY chunk_index ASC"
    )
    .bind(note_id)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Failed to load chunks".into()))?;

    let mut tx = state.pool.begin().await.map_err(|_| {
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Transaction error".into())
    })?;

    let new_uuid = uuid::Uuid::new_v4().to_string();
    let mut new_title = source_note.title.clone();
    if new_title.chars().count() > 240 {
        new_title = new_title.chars().take(240).collect();
    }
    new_title.push_str(" (Copy)");

    let insert_res = sqlx::query(
        r#"
        INSERT INTO notes 
            (user_id, folder_id, uuid, title, category, content, font_family, font_size, theme_color, word_count, char_count, is_pinned, is_favorite, level, xp, created_at, updated_at)
        VALUES 
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, NOW(), NOW())
        "#,
    )
    .bind(user.id)
    .bind(source_note.folder_id)
    .bind(&new_uuid)
    .bind(&new_title)
    .bind(&source_note.category)
    .bind(&source_note.content)
    .bind(&source_note.font_family)
    .bind(&source_note.font_size)
    .bind(&source_note.theme_color)
    .bind(source_note.word_count)
    .bind(source_note.char_count)
    .bind(source_note.is_favorite)
    .bind(source_note.level)
    .bind(source_note.xp)
    .execute(&mut *tx)
    .await
    .map_err(|e| {
        tracing::error!("Failed to duplicate note: {:?}", e);
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Failed to duplicate note".into())
    })?;

    let new_note_id = insert_res.last_insert_id();

    let mut cloned_chunks = Vec::new();
    for ch in source_chunks {
        let new_chunk_uid = crate::chunker::generate_chunk_uid();
        let meta_json = ch.metadata.as_ref().map(|m| m.to_string());
        let chunk_res = sqlx::query(
            r#"
            INSERT INTO note_chunks 
                (note_id, chunk_uid, chunk_index, chunk_type, content, byte_size, checksum, metadata, created_at, updated_at)
            VALUES 
                (?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
            "#,
        )
        .bind(new_note_id)
        .bind(&new_chunk_uid)
        .bind(ch.chunk_index)
        .bind(&ch.chunk_type)
        .bind(&ch.content)
        .bind(ch.byte_size)
        .bind(&ch.checksum)
        .bind(meta_json)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            tracing::error!("Failed to duplicate chunk: {:?}", e);
            ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Failed to duplicate chunks".into())
        })?;

        cloned_chunks.push(NoteChunk {
            id: chunk_res.last_insert_id(),
            note_id: new_note_id,
            chunk_uid: new_chunk_uid,
            chunk_index: ch.chunk_index,
            chunk_type: ch.chunk_type,
            content: ch.content,
            byte_size: ch.byte_size,
            checksum: ch.checksum,
            metadata: ch.metadata,
            created_at: Some(chrono::Utc::now()),
            updated_at: Some(chrono::Utc::now()),
        });
    }

    tx.commit().await.map_err(|_| {
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Commit failed".into())
    })?;

    let cloned_note = Note {
        id: new_note_id,
        user_id: user.id,
        folder_id: source_note.folder_id,
        uuid: new_uuid,
        title: new_title,
        category: source_note.category,
        content: source_note.content,
        font_family: source_note.font_family,
        font_size: source_note.font_size,
        theme_color: source_note.theme_color,
        word_count: source_note.word_count,
        char_count: source_note.char_count,
        is_pinned: false,
        is_favorite: source_note.is_favorite,
        level: source_note.level,
        xp: source_note.xp,
        created_at: Some(chrono::Utc::now()),
        updated_at: Some(chrono::Utc::now()),
    };

    state
        .cache
        .insert_note(user.id, cloned_note.clone(), cloned_chunks.clone())
        .await;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "status": "success",
            "message": "Scroll duplicated into the memory matrix!",
            "data": NoteWithChunks {
                reading_time: cloned_note.reading_time(),
                note: cloned_note,
                chunks: cloned_chunks,
            }
        })),
    ))
}

pub async fn update_chunk(
    State(state): State<AppState>,
    parts: Parts,
    Path((note_id, chunk_id)): Path<(u64, u64)>,
    Json(payload): Json<UpdateChunkPayload>,
) -> Result<impl IntoResponse, ApiError> {
    let user = authenticate(&parts, &state.pool, &state.cache)
        .await
        .map_err(|e| ApiError(StatusCode::UNAUTHORIZED, format!("{:?}", e)))?;

    // Verify note ownership
    let _ = sqlx::query("SELECT id FROM notes WHERE id = ? AND user_id = ?")
        .bind(note_id)
        .bind(user.id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Database error".into()))?
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "Note not found".into()))?;

    let chunk_type = payload
        .chunk_type
        .unwrap_or_else(|| detect_chunk_type(&payload.content));
    let byte_size = payload.content.len() as u32;
    let checksum = compute_checksum(&payload.content);

    let mut tx = state.pool.begin().await.map_err(|_| {
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Transaction error".into())
    })?;

    let update_res = sqlx::query(
        r#"
        UPDATE note_chunks
        SET content = ?, chunk_type = ?, byte_size = ?, checksum = ?, updated_at = NOW()
        WHERE id = ? AND note_id = ?
        "#,
    )
    .bind(&payload.content)
    .bind(&chunk_type)
    .bind(byte_size)
    .bind(&checksum)
    .bind(chunk_id)
    .bind(note_id)
    .execute(&mut *tx)
    .await
    .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Chunk update failed".into()))?;

    if update_res.rows_affected() == 0 {
        return Err(ApiError(StatusCode::NOT_FOUND, "Chunk not found".into()));
    }

    // Reassemble full note content
    let all_chunks = sqlx::query_as::<_, NoteChunk>(
        "SELECT id, note_id, chunk_uid, chunk_index, chunk_type, content, byte_size, checksum, metadata, created_at, updated_at FROM note_chunks WHERE note_id = ? ORDER BY chunk_index ASC"
    )
    .bind(note_id)
    .fetch_all(&mut *tx)
    .await
    .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Failed to load chunks".into()))?;

    let reassembled = all_chunks
        .iter()
        .map(|c| c.content.as_str())
        .collect::<Vec<_>>()
        .join("");

    let metrics = calculate_metrics(&reassembled);

    sqlx::query(
        r#"
        UPDATE notes
        SET content = ?, word_count = ?, char_count = ?, xp = ?, level = ?, updated_at = NOW()
        WHERE id = ?
        "#,
    )
    .bind(&reassembled)
    .bind(metrics.word_count)
    .bind(metrics.char_count)
    .bind(metrics.xp)
    .bind(metrics.level)
    .bind(note_id)
    .execute(&mut *tx)
    .await
    .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Note update failed".into()))?;

    tx.commit().await.map_err(|_| {
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Commit failed".into())
    })?;

    state.cache.invalidate_note(user.id, note_id).await;

    let updated_chunk = all_chunks.into_iter().find(|c| c.id == chunk_id);

    Ok(Json(serde_json::json!({
        "status": "success",
        "message": format!("Shard {} updated & checksum recomputed!", chunk_id),
        "data": {
            "chunk": updated_chunk,
            "note_id": note_id,
        }
    })))
}

pub async fn delete_chunk(
    State(state): State<AppState>,
    parts: Parts,
    Path((note_id, chunk_id)): Path<(u64, u64)>,
) -> Result<impl IntoResponse, ApiError> {
    let user = authenticate(&parts, &state.pool, &state.cache)
        .await
        .map_err(|e| ApiError(StatusCode::UNAUTHORIZED, format!("{:?}", e)))?;

    // Verify note ownership
    let _ = sqlx::query("SELECT id FROM notes WHERE id = ? AND user_id = ?")
        .bind(note_id)
        .bind(user.id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Database error".into()))?
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "Note not found".into()))?;

    let mut tx = state.pool.begin().await.map_err(|_| {
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Transaction error".into())
    })?;

    let del_res = sqlx::query("DELETE FROM note_chunks WHERE id = ? AND note_id = ?")
        .bind(chunk_id)
        .bind(note_id)
        .execute(&mut *tx)
        .await
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Delete failed".into()))?;

    if del_res.rows_affected() == 0 {
        return Err(ApiError(StatusCode::NOT_FOUND, "Chunk not found".into()));
    }

    // Re-index remaining chunks
    let remaining = sqlx::query_as::<_, NoteChunk>(
        "SELECT id, note_id, chunk_uid, chunk_index, chunk_type, content, byte_size, checksum, metadata, created_at, updated_at FROM note_chunks WHERE note_id = ? ORDER BY chunk_index ASC"
    )
    .bind(note_id)
    .fetch_all(&mut *tx)
    .await
    .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Query failed".into()))?;

    for (new_idx, ch) in remaining.iter().enumerate() {
        if ch.chunk_index != new_idx as u32 {
            sqlx::query("UPDATE note_chunks SET chunk_index = ? WHERE id = ?")
                .bind(new_idx as u32)
                .bind(ch.id)
                .execute(&mut *tx)
                .await
                .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Reindex failed".into()))?;
        }
    }

    // Reassemble and update note
    let reassembled = remaining
        .iter()
        .map(|c| c.content.as_str())
        .collect::<Vec<_>>()
        .join("");

    let metrics = calculate_metrics(&reassembled);

    sqlx::query(
        r#"
        UPDATE notes
        SET content = ?, word_count = ?, char_count = ?, xp = ?, level = ?, updated_at = NOW()
        WHERE id = ?
        "#,
    )
    .bind(&reassembled)
    .bind(metrics.word_count)
    .bind(metrics.char_count)
    .bind(metrics.xp)
    .bind(metrics.level)
    .bind(note_id)
    .execute(&mut *tx)
    .await
    .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Note update failed".into()))?;

    tx.commit().await.map_err(|_| {
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Commit failed".into())
    })?;

    state.cache.invalidate_note(user.id, note_id).await;

    Ok(Json(serde_json::json!({
        "status": "success",
        "message": "Chunk deleted and scroll reassembled!"
    })))
}

pub async fn reorder_chunks(
    State(state): State<AppState>,
    parts: Parts,
    Path(note_id): Path<u64>,
    Json(payload): Json<ReorderChunksPayload>,
) -> Result<impl IntoResponse, ApiError> {
    let user = authenticate(&parts, &state.pool, &state.cache)
        .await
        .map_err(|e| ApiError(StatusCode::UNAUTHORIZED, format!("{:?}", e)))?;

    // Verify note ownership
    let _ = sqlx::query("SELECT id FROM notes WHERE id = ? AND user_id = ?")
        .bind(note_id)
        .bind(user.id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Database error".into()))?
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "Note not found".into()))?;

    let mut tx = state.pool.begin().await.map_err(|_| {
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Transaction error".into())
    })?;

    for (new_index, chunk_id) in payload.order.iter().enumerate() {
        sqlx::query("UPDATE note_chunks SET chunk_index = ? WHERE id = ? AND note_id = ?")
            .bind(new_index as u32)
            .bind(chunk_id)
            .bind(note_id)
            .execute(&mut *tx)
            .await
            .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Reorder failed".into()))?;
    }

    // Reassemble in new order
    let reordered = sqlx::query_as::<_, NoteChunk>(
        "SELECT id, note_id, chunk_uid, chunk_index, chunk_type, content, byte_size, checksum, metadata, created_at, updated_at FROM note_chunks WHERE note_id = ? ORDER BY chunk_index ASC"
    )
    .bind(note_id)
    .fetch_all(&mut *tx)
    .await
    .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Failed to load chunks".into()))?;

    let reassembled = reordered
        .iter()
        .map(|c| c.content.as_str())
        .collect::<Vec<_>>()
        .join("");

    let metrics = calculate_metrics(&reassembled);

    sqlx::query(
        r#"
        UPDATE notes
        SET content = ?, word_count = ?, char_count = ?, xp = ?, level = ?, updated_at = NOW()
        WHERE id = ?
        "#,
    )
    .bind(&reassembled)
    .bind(metrics.word_count)
    .bind(metrics.char_count)
    .bind(metrics.xp)
    .bind(metrics.level)
    .bind(note_id)
    .execute(&mut *tx)
    .await
    .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Note update failed".into()))?;

    tx.commit().await.map_err(|_| {
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Commit failed".into())
    })?;

    state.cache.invalidate_note(user.id, note_id).await;

    Ok(Json(serde_json::json!({
        "status": "success",
        "message": "Chunks successfully reordered and note reassembled!"
    })))
}
