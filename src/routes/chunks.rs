use crate::{
    chunker::{calculate_metrics, compute_checksum, detect_chunk_type, split_into_chunks},
    models::{
        AuthUser, ChunksQuery, NoteChunk, ReorderChunksPayload, UpdateChunkPayload,
        UpdateChunksPayload,
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
use futures_util::TryStreamExt;
use serde::Serialize;

#[derive(Serialize)]
struct ChunkPageResponse {
    status: &'static str,
    note_id: u64,
    note_uuid: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    total_chunks: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    total_bytes: Option<i64>,
    offset: u32,
    limit: u32,
    has_more: bool,
    chunks: Vec<NoteChunk>,
}

#[derive(Serialize)]
struct UpdatedChunkData {
    chunk: NoteChunk,
    note_id: u64,
}

#[derive(Serialize)]
struct UpdatedChunkResponse {
    status: &'static str,
    message: String,
    data: UpdatedChunkData,
}

// Limit normal pages by bytes as well as by chunk count. A single legacy chunk
// may exceed this budget and must still be returned intact.
const PAGE_BYTE_BUDGET: u64 = 256 * 1024;

fn chunk_page_query(note_id: u64, user_id: u64, query: &ChunksQuery, select: &str, limit: u32) -> sqlx::QueryBuilder<'static, sqlx::MySql> {
    let mut builder = sqlx::QueryBuilder::<sqlx::MySql>::new(select.to_owned());
    builder.push_bind(note_id);
    builder.push(" AND EXISTS (SELECT 1 FROM notes WHERE notes.id = note_chunks.note_id AND notes.user_id = ")
        .push_bind(user_id).push(")");
    if let (Some(index), Some(id)) = (query.after_index, query.after_id) {
        builder.push(" AND (chunk_index > ").push_bind(index)
            .push(" OR (chunk_index = ").push_bind(index)
            .push(" AND id > ").push_bind(id).push("))");
    }
    builder.push(" ORDER BY chunk_index ASC, id ASC LIMIT ").push_bind(limit);
    if query.after_id.is_none() && query.offset.unwrap_or(0) > 0 {
        builder.push(" OFFSET ").push_bind(query.offset.unwrap_or(0));
    }
    builder
}

pub async fn get_chunks(
    State(state): State<AppState>,
    user: AuthUser,
    Path(note_id): Path<u64>,
    Query(query): Query<ChunksQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let offset = query.offset.unwrap_or(0);
    let limit = query.limit.unwrap_or(2);
    if limit == 0 || limit > 100 {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "limit must be between 1 and 100".into(),
        ));
    }
    if query.lightweight.unwrap_or(false) {
        if query.after_index.is_some() != query.after_id.is_some() {
            return Err(ApiError(StatusCode::BAD_REQUEST, "Both cursor fields are required".into()));
        }
        if query.after_id.is_some() && offset != 0 {
            return Err(ApiError(StatusCode::BAD_REQUEST, "Cursor cannot be combined with offset".into()));
        }
        // Fetch index metadata first: looking ahead must not download an
        // arbitrarily large LONGTEXT chunk just to determine has_more.
        let (uuid, sizes) = if offset == 0 {
            // One indexed query verifies ownership and reads only small chunk metadata.
            let mut builder = sqlx::QueryBuilder::<sqlx::MySql>::new(
                "SELECT n.uuid, c.chunk_index, c.id, c.byte_size FROM notes n LEFT JOIN note_chunks c ON c.note_id = n.id"
            );
            if let (Some(index), Some(id)) = (query.after_index, query.after_id) {
                builder.push(" AND (c.chunk_index > ").push_bind(index)
                    .push(" OR (c.chunk_index = ").push_bind(index)
                    .push(" AND c.id > ").push_bind(id).push("))");
            }
            builder.push(" WHERE n.id = ").push_bind(note_id)
                .push(" AND n.user_id = ").push_bind(user.id)
                .push(" ORDER BY c.chunk_index ASC, c.id ASC LIMIT ").push_bind(limit + 1);
            let rows = builder.build_query_as::<(String, Option<u32>, Option<u64>, Option<u32>)>()
                .fetch_all(&state.pool).await
                .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Failed to load chunk metadata".into()))?;
            let uuid = rows.first().map(|row| row.0.clone())
                .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "Note not found".into()))?;
            let sizes: Vec<(u32, u64, u32)> = rows.into_iter()
                .filter_map(|(_, index, id, size)| Some((index?, id?, size?))).collect();
            (uuid, sizes)
        } else {
            // Preserve the legacy offset form, including 404 for missing notes.
            let (uuid,): (String,) = sqlx::query_as("SELECT uuid FROM notes WHERE id = ? AND user_id = ?")
                .bind(note_id).bind(user.id).fetch_optional(&state.pool).await
                .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Database error".into()))?
                .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "Note not found".into()))?;
            let sizes = chunk_page_query(note_id, user.id, &query,
                "SELECT chunk_index, id, byte_size FROM note_chunks WHERE note_id = ", limit + 1)
                .build_query_as::<(u32, u64, u32)>()
                .fetch_all(&state.pool).await
                .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Failed to load chunk metadata".into()))?;
            (uuid, sizes)
        };
        let mut page_size = 0u32;
        let mut total = 0u64;
        for &(_, _, size) in sizes.iter().take(limit as usize) {
            if page_size > 0 && total + u64::from(size) > PAGE_BYTE_BUDGET {
                break;
            }
            total += u64::from(size);
            page_size += 1;
        }
        let chunks = if page_size == 0 {
            Vec::new()
        } else {
            chunk_page_query(note_id, user.id, &query,
                "SELECT id, note_id, chunk_uid, chunk_index, chunk_type, content, byte_size, checksum, metadata, created_at, updated_at FROM note_chunks WHERE note_id = ", page_size)
                .build_query_as::<NoteChunk>().fetch_all(&state.pool).await
                .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Failed to load chunks".into()))?
        };
        let has_more = !chunks.is_empty() && sizes.len() > chunks.len();
        return Ok(Json(ChunkPageResponse {
            status: "success", note_id, note_uuid: uuid,
            total_chunks: None, total_bytes: None,
            offset, limit, has_more, chunks,
        }));
    }
    if query.after_index.is_some() || query.after_id.is_some() {
        return Err(ApiError(StatusCode::BAD_REQUEST, "Cursor requires lightweight mode".into()));
    }
    let (uuid,): (String,) = sqlx::query_as("SELECT uuid FROM notes WHERE id = ? AND user_id = ?")
        .bind(note_id).bind(user.id).fetch_optional(&state.pool).await
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Database error".into()))?
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "Note not found".into()))?;
    let (total_chunks, total_bytes): (i64, i64) = sqlx::query_as(
        "SELECT COUNT(*), CAST(COALESCE(SUM(byte_size), 0) AS SIGNED) FROM note_chunks WHERE note_id = ?"
    ).bind(note_id).fetch_one(&state.pool).await
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Database error".into()))?;
    let cached = if offset == 0 && limit == 2 {
        state
            .cache
            .get_initial_chunks(user.id, note_id)
            .await
            .map(|p| (*p).clone())
    } else {
        state
            .cache
            .get_page(user.id, note_id, offset, limit)
            .await
            .map(|p| p.chunks.clone())
    };
    let chunks = match cached {
        Some(chunks) => chunks,
        None => {
            let chunks = sqlx::query_as::<_, NoteChunk>(
                "SELECT id, note_id, chunk_uid, chunk_index, chunk_type, content, byte_size, checksum, metadata, created_at, updated_at FROM note_chunks WHERE note_id = ? ORDER BY chunk_index ASC, id ASC LIMIT ? OFFSET ?"
            ).bind(note_id).bind(limit).bind(offset).fetch_all(&state.pool).await
                .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Failed to load chunks".into()))?;
            if offset == 0 && limit == 2 {
                state
                    .cache
                    .insert_initial_chunks(user.id, note_id, chunks.clone())
                    .await;
            } else {
                state
                    .cache
                    .insert_page(
                        user.id,
                        note_id,
                        crate::cache::CachedPage {
                            offset,
                            limit,
                            chunks: chunks.clone(),
                        },
                    )
                    .await;
            }
            chunks
        }
    };
    let has_more = u64::from(offset) + (chunks.len() as u64) < total_chunks as u64;
    Ok(Json(ChunkPageResponse {
        status: "success", note_id, note_uuid: uuid,
        total_chunks: Some(total_chunks), total_bytes: Some(total_bytes),
        offset, limit, has_more, chunks,
    }))
}

/// PUT /api/notes/{note_id}/chunks: updates or synchronizes chunks for a note and invalidates cache
pub async fn update_chunks(
    State(state): State<AppState>,
    user: AuthUser,
    Path(note_id): Path<u64>,
    Json(payload): Json<UpdateChunksPayload>,
) -> Result<axum::response::Response, ApiError> {
    // 1. Verify note ownership
    let _ = sqlx::query("SELECT id FROM notes WHERE id = ? AND user_id = ?")
        .bind(note_id)
        .bind(user.id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Database error".into()))?
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "Note not found".into()))?;

    let mut tx = state.pool.begin().await.map_err(|_| {
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Transaction error".into(),
        )
    })?;

    // Case A: Provided content string to re-chunk
    if let Some(content) = payload.content {
        sqlx::query("DELETE FROM note_chunks WHERE note_id = ?")
            .bind(note_id)
            .execute(&mut *tx)
            .await
            .map_err(|_| {
                ApiError(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to clear chunks".into(),
                )
            })?;

        let drafts = split_into_chunks(&content);
        let new_chunks = crate::db::insert_chunk_drafts(&mut tx, note_id, &drafts)
            .await
            .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Chunk insert failed".into()))?;

        let metrics = calculate_metrics(&content);
        sqlx::query(
            "UPDATE notes SET content = ?, word_count = ?, char_count = ?, xp = ?, level = ?, updated_at = NOW() WHERE id = ?"
        )
        .bind(&content)
        .bind(metrics.word_count)
        .bind(metrics.char_count)
        .bind(metrics.xp)
        .bind(metrics.level)
        .bind(note_id)
        .execute(&mut *tx)
        .await
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Note update failed".into()))?;

        tx.commit()
            .await
            .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Commit failed".into()))?;

        // Invalidate cache
        state.cache.invalidate_note(user.id, note_id).await;

        return Ok(Json(serde_json::json!({
            "status": "success",
            "message": "Chunks successfully re-created and cached!",
            "note_id": note_id,
            "chunks": new_chunks
        }))
        .into_response());
    }

    // Case B: Provided explicit chunk array
    if let Some(mut chunks) = payload.chunks {
        for (index, chunk) in chunks.iter_mut().enumerate() {
            chunk.chunk_index.get_or_insert(index as u32);
        }
        chunks.sort_by_key(|chunk| chunk.chunk_index);
        if chunks
            .windows(2)
            .any(|pair| pair[0].chunk_index == pair[1].chunk_index)
        {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                "Duplicate chunk index".into(),
            ));
        }
        sqlx::query("DELETE FROM note_chunks WHERE note_id = ?")
            .bind(note_id)
            .execute(&mut *tx)
            .await
            .map_err(|_| {
                ApiError(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to clear chunks".into(),
                )
            })?;

        let mut reassembled_text = String::new();
        let drafts: Vec<crate::chunker::ChunkDraft> = chunks.into_iter().enumerate().map(|(index, chunk)| {
            reassembled_text.push_str(&chunk.content);
            crate::chunker::ChunkDraft {
                chunk_uid: crate::chunker::generate_chunk_uid(),
                chunk_index: chunk.chunk_index.unwrap_or(index as u32),
                chunk_type: chunk.chunk_type.unwrap_or_else(|| detect_chunk_type(&chunk.content)),
                byte_size: chunk.content.len() as u32,
                checksum: compute_checksum(&chunk.content),
                content: chunk.content,
                metadata: None,
            }
        }).collect();
        let inserted_chunks = crate::db::insert_chunk_drafts(&mut tx, note_id, &drafts)
            .await
            .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Chunk insert failed".into()))?;

        let metrics = calculate_metrics(&reassembled_text);
        sqlx::query(
            "UPDATE notes SET content = ?, word_count = ?, char_count = ?, xp = ?, level = ?, updated_at = NOW() WHERE id = ?"
        )
        .bind(&reassembled_text)
        .bind(metrics.word_count)
        .bind(metrics.char_count)
        .bind(metrics.xp)
        .bind(metrics.level)
        .bind(note_id)
        .execute(&mut *tx)
        .await
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Note update failed".into()))?;

        tx.commit()
            .await
            .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Commit failed".into()))?;

        state.cache.invalidate_note(user.id, note_id).await;

        return Ok(Json(serde_json::json!({
            "status": "success",
            "message": "Chunks successfully replaced!",
            "note_id": note_id,
            "chunks": inserted_chunks
        }))
        .into_response());
    }

    Err(ApiError(
        StatusCode::BAD_REQUEST,
        "Provide either 'content' or 'chunks' array to update".into(),
    ))
}

pub async fn update_single_chunk(
    State(state): State<AppState>,
    user: AuthUser,
    Path((note_id, chunk_id)): Path<(u64, u64)>,
    Json(payload): Json<UpdateChunkPayload>,
) -> Result<axum::response::Response, ApiError> {
    let chunk_type = payload
        .chunk_type
        .unwrap_or_else(|| detect_chunk_type(&payload.content));
    let byte_size = payload.content.len() as u32;
    let checksum = compute_checksum(&payload.content);

    let mut tx = state.pool.begin().await.map_err(|_| {
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Transaction error".into(),
        )
    })?;

    // Serialize concurrent chunk edits for the same note before reassembling it.
    sqlx::query("SELECT id FROM notes WHERE id = ? AND user_id = ? FOR UPDATE")
        .bind(note_id)
        .bind(user.id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Database error".into()))?
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "Note not found".into()))?;

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
    .map_err(|_| {
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Chunk update failed".into(),
        )
    })?;

    if update_res.rows_affected() == 0 {
        return Err(ApiError(StatusCode::NOT_FOUND, "Chunk not found".into()));
    }

    // Stream only content instead of buffering every full chunk record.
    let mut reassembled = String::new();
    {
        let mut rows = sqlx::query_as::<_, (String,)>(
            "SELECT content FROM note_chunks WHERE note_id = ? ORDER BY chunk_index ASC, id ASC"
        ).bind(note_id).fetch(&mut *tx);
        while let Some((content,)) = rows.try_next().await
            .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Failed to load chunks".into()))?
        {
            reassembled.push_str(&content);
        }
    }

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
    .map_err(|_| {
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Note update failed".into(),
        )
    })?;

    let updated_chunk = sqlx::query_as::<_, NoteChunk>(
        "SELECT id, note_id, chunk_uid, chunk_index, chunk_type, content, byte_size, checksum, metadata, created_at, updated_at FROM note_chunks WHERE id = ? AND note_id = ?"
    ).bind(chunk_id).bind(note_id).fetch_one(&mut *tx).await
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Failed to load chunk".into()))?;

    tx.commit()
        .await
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Commit failed".into()))?;

    // Invalidate Moka cache
    state.cache.invalidate_note(user.id, note_id).await;

    Ok(Json(UpdatedChunkResponse {
        status: "success",
        message: format!("Shard {} updated & checksum recomputed!", chunk_id),
        data: UpdatedChunkData { chunk: updated_chunk, note_id },
    })
    .into_response())
}

pub async fn delete_chunk(
    State(state): State<AppState>,
    user: AuthUser,
    Path((note_id, chunk_id)): Path<(u64, u64)>,
) -> Result<impl IntoResponse, ApiError> {
    let _ = sqlx::query("SELECT id FROM notes WHERE id = ? AND user_id = ?")
        .bind(note_id)
        .bind(user.id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Database error".into()))?
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "Note not found".into()))?;

    let mut tx = state.pool.begin().await.map_err(|_| {
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Transaction error".into(),
        )
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
                .map_err(|_| {
                    ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Reindex failed".into())
                })?;
        }
    }

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
    .map_err(|_| {
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Note update failed".into(),
        )
    })?;

    tx.commit()
        .await
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Commit failed".into()))?;

    // Invalidate cache
    state.cache.invalidate_note(user.id, note_id).await;

    Ok(Json(serde_json::json!({
        "status": "success",
        "message": "Chunk deleted and note reassembled!"
    })))
}

pub async fn reorder_chunks(
    State(state): State<AppState>,
    user: AuthUser,
    Path(note_id): Path<u64>,
    Json(payload): Json<ReorderChunksPayload>,
) -> Result<impl IntoResponse, ApiError> {
    let _ = sqlx::query("SELECT id FROM notes WHERE id = ? AND user_id = ?")
        .bind(note_id)
        .bind(user.id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Database error".into()))?
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "Note not found".into()))?;

    let mut tx = state.pool.begin().await.map_err(|_| {
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Transaction error".into(),
        )
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
    .map_err(|_| {
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Note update failed".into(),
        )
    })?;

    tx.commit()
        .await
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Commit failed".into()))?;

    state.cache.invalidate_note(user.id, note_id).await;

    Ok(Json(serde_json::json!({
        "status": "success",
        "message": "Chunks successfully reordered and note reassembled!",
        "data": {
            "chunks": reordered
        }
    })))
}

pub async fn verify_chunks(
    State(state): State<AppState>,
    user: AuthUser,
    Path(note_id): Path<u64>,
) -> Result<impl IntoResponse, ApiError> {
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
