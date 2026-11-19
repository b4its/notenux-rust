use crate::{
    chunker::{calculate_metrics, generate_chunk_uid, split_into_chunks},
    models::{
        AuthUser, CreateNotePayload, Note, NoteChunk, NoteSummary, NoteSummaryRow, NoteWithChunks,
        NotesListQuery, UpdateNotePayload,
    },
    routes::ApiError,
    AppState,
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use sqlx::MySql;
use serde::Serialize;

#[derive(Serialize)]
struct NoteDetailResponse {
    status: &'static str,
    data: NoteWithChunks,
}

pub async fn list_notes(
    State(state): State<AppState>,
    user: AuthUser,
    Query(query): Query<NotesListQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let has_filter = query.q.is_some()
        || query.category.is_some()
        || query.folder_id.is_some()
        || query.pinned_only.is_some()
        || query.favorites_only.is_some()
        || query.sort.is_some();

    // Fast path: cached metadata list if no filters
    if !has_filter {
        if let Some(cached) = state.cache.get_notes_list(user.id).await {
            return Ok(Json(serde_json::json!({
                "status": "success",
                "data": cached.as_ref()
            })));
        }
    }

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

    // Cache unfiltered default list
    if !has_filter {
        state
            .cache
            .insert_notes_list(user.id, summaries.clone())
            .await;
    }

    Ok(Json(serde_json::json!({
        "status": "success",
        "data": summaries
    })))
}

pub async fn get_note(
    State(state): State<AppState>,
    user: AuthUser,
    Path(note_id): Path<u64>,
) -> Result<impl IntoResponse, ApiError> {
    // Check Moka bounded cache
    if let Some(cached) = state.cache.get_note(user.id, note_id).await {
        return Ok(Json(NoteDetailResponse {
            status: "success",
            data: NoteWithChunks {
                reading_time: cached.note.reading_time(),
                note: cached.note.clone(),
                chunks: cached.chunks.clone(),
            },
        }));
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

    // Store in Moka cache (including initial chunks priority)
    state
        .cache
        .insert_note(user.id, &note, &chunks)
        .await;

    Ok(Json(NoteDetailResponse {
        status: "success",
        data: NoteWithChunks {
            reading_time: note.reading_time(),
            note,
            chunks,
        },
    }))
}

pub async fn create_note(
    State(state): State<AppState>,
    user: AuthUser,
    Json(payload): Json<CreateNotePayload>,
) -> Result<impl IntoResponse, ApiError> {
    let raw_title = payload.title.as_deref().unwrap_or("Catatan Baru").trim();
    let title = if raw_title.is_empty() {
        "Catatan Baru"
    } else {
        raw_title
    };
    let category = payload.category.as_deref().unwrap_or("catatan").trim();
    let content = payload.content.unwrap_or_default();
    let font_family = payload.font_family.as_deref().unwrap_or("pixel");
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

    let created_chunks = crate::db::insert_chunk_drafts(&mut tx, note_id, &chunk_payloads)
        .await
        .map_err(|e| {
            tracing::error!("Failed to insert chunks: {:?}", e);
            ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Failed to create chunks".into())
        })?;

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

    // Store in Moka Cache and clear user notes list cache
    state
        .cache
        .insert_note(user.id, &created_note, &created_chunks)
        .await;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "status": "success",
            "message": "Note created and chunked into memory matrix!",
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
    user: AuthUser,
    Path(note_id): Path<u64>,
    Json(payload): Json<UpdateNotePayload>,
) -> Result<impl IntoResponse, ApiError> {
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

    let updated_chunks;
    if content_changed {
        sqlx::query("DELETE FROM note_chunks WHERE note_id = ?")
            .bind(note_id)
            .execute(&mut *tx)
            .await
            .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Chunk sync failed".into()))?;

        let drafts = split_into_chunks(&new_content);
        updated_chunks = crate::db::insert_chunk_drafts(&mut tx, note_id, &drafts)
            .await
            .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Chunk insert failed".into()))?;
    } else {
        updated_chunks = sqlx::query_as::<_, NoteChunk>(
            "SELECT id, note_id, chunk_uid, chunk_index, chunk_type, content, byte_size, checksum, metadata, created_at, updated_at FROM note_chunks WHERE note_id = ? ORDER BY chunk_index ASC"
        )
        .bind(note_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Chunk query failed".into()))?;
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

    // Update Moka cache
    state
        .cache
        .insert_note(user.id, &current_note, &updated_chunks)
        .await;

    Ok(Json(serde_json::json!({
        "status": "success",
        "message": "Note updated and chunks harmonized!",
        "data": NoteWithChunks {
            reading_time: current_note.reading_time(),
            note: current_note,
            chunks: updated_chunks,
        }
    })))
}

pub async fn delete_note(
    State(state): State<AppState>,
    user: AuthUser,
    Path(note_id): Path<u64>,
) -> Result<impl IntoResponse, ApiError> {
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

    // Invalidate Moka cache
    state.cache.invalidate_note(user.id, note_id).await;

    Ok(Json(serde_json::json!({
        "status": "success",
        "message": "Note deleted successfully!"
    })))
}

pub async fn toggle_pin(
    State(state): State<AppState>,
    user: AuthUser,
    Path(note_id): Path<u64>,
) -> Result<impl IntoResponse, ApiError> {
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
        "message": if updated_pinned.0 { "Note pinned!" } else { "Note unpinned!" }
    })))
}

pub async fn toggle_favorite(
    State(state): State<AppState>,
    user: AuthUser,
    Path(note_id): Path<u64>,
) -> Result<impl IntoResponse, ApiError> {
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
        "message": if updated_fav.0 { "Note favorited!" } else { "Note unfavorited!" }
    })))
}

pub async fn duplicate_note(
    State(state): State<AppState>,
    user: AuthUser,
    Path(note_id): Path<u64>,
) -> Result<impl IntoResponse, ApiError> {
    let source_note = sqlx::query_as::<_, Note>(
        "SELECT id, user_id, folder_id, uuid, title, category, content, font_family, font_size, theme_color, word_count, char_count, is_pinned, is_favorite, level, xp, created_at, updated_at FROM notes WHERE id = ? AND user_id = ?"
    )
    .bind(note_id)
    .bind(user.id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Database error".into()))?
    .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "Note not found".into()))?;

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
        let new_chunk_uid = generate_chunk_uid();
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
        .insert_note(user.id, &cloned_note, &cloned_chunks)
        .await;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "status": "success",
            "message": "Note duplicated successfully!",
            "data": NoteWithChunks {
                reading_time: cloned_note.reading_time(),
                note: cloned_note,
                chunks: cloned_chunks,
            }
        })),
    ))
}
