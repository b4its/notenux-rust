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

pub async fn get_chunks(
    State(state): State<AppState>,
    user: AuthUser,
    Path(note_id): Path<u64>,
    Query(query): Query<ChunksQuery>,
) -> Result<impl IntoResponse, ApiError> {
    // 1. Verify note ownership
    let (uuid,): (String,) = sqlx::query_as("SELECT uuid FROM notes WHERE id = ? AND user_id = ?")
        .bind(note_id)
        .bind(user.id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Database error".into()))?
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "Note not found".into()))?;

    let offset = query.offset.unwrap_or(0) as usize;

    // Fast-path for initial chunks (offset = 0 and limit <= 2)
    if offset == 0 && query.limit.is_some_and(|l| l <= 2) {
        if let Some(initials) = state.cache.get_initial_chunks(user.id, note_id).await {
            let limit = query.limit.unwrap() as usize;
            let end = limit.min(initials.len());
            let sliced = initials[0..end].to_vec();
            let total_chunks: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM note_chunks WHERE note_id = ?")
                .bind(note_id)
                .fetch_one(&state.pool)
                .await
                .unwrap_or((initials.len() as i64,));

            let total_c = total_chunks.0 as usize;
            let total_bytes: u32 = initials.iter().map(|c| c.byte_size).sum();

            return Ok(Json(serde_json::json!({
                "status": "success",
                "note_id": note_id,
                "note_uuid": uuid,
                "total_chunks": total_c,
                "total_bytes": total_bytes,
                "offset": 0,
                "limit": limit,
                "has_more": limit < total_c,
                "chunks": sliced,
            })));
        }
    }

    // Check full chunks cache
    let all_chunks = if let Some(cached) = state.cache.get_note(user.id, note_id).await {
        cached.chunks.clone()
    } else {
        // Query from MySQL
        let db_chunks = sqlx::query_as::<_, NoteChunk>(
            "SELECT id, note_id, chunk_uid, chunk_index, chunk_type, content, byte_size, checksum, metadata, created_at, updated_at FROM note_chunks WHERE note_id = ? ORDER BY chunk_index ASC"
        )
        .bind(note_id)
        .fetch_all(&state.pool)
        .await
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Failed to load chunks".into()))?;

        // Cache initial chunks (0 and 1) for prioritized access
        let initials: Vec<NoteChunk> = db_chunks
            .iter()
            .filter(|c| c.chunk_index <= 1)
            .cloned()
            .collect();
        if !initials.is_empty() {
            state
                .cache
                .insert_initial_chunks(user.id, note_id, initials)
                .await;
        }

        db_chunks
    };

    let total_chunks = all_chunks.len();
    let total_bytes: u32 = all_chunks.iter().map(|c| c.byte_size).sum();

    // Sliced chunk response if limit is specified
    if let Some(limit) = query.limit {
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

    // Default: full list of chunks
    Ok(Json(serde_json::json!({
        "status": "success",
        "note_id": note_id,
        "note_uuid": uuid,
        "total_chunks": total_chunks,
        "total_bytes": total_bytes,
        "chunks": all_chunks,
    })))
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
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Transaction error".into())
    })?;

    // Case A: Provided content string to re-chunk
    if let Some(content) = payload.content {
        sqlx::query("DELETE FROM note_chunks WHERE note_id = ?")
            .bind(note_id)
            .execute(&mut *tx)
            .await
            .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Failed to clear chunks".into()))?;

        let drafts = split_into_chunks(&content);
        let mut new_chunks = Vec::new();
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

            new_chunks.push(NoteChunk {
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

        tx.commit().await.map_err(|_| {
            ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Commit failed".into())
        })?;

        // Invalidate cache
        state.cache.invalidate_note(user.id, note_id).await;

        return Ok(Json(serde_json::json!({
            "status": "success",
            "message": "Chunks successfully re-created and cached!",
            "note_id": note_id,
            "chunks": new_chunks
        })).into_response());
    }

    // Case B: Provided explicit chunk array
    if let Some(chunks) = payload.chunks {
        sqlx::query("DELETE FROM note_chunks WHERE note_id = ?")
            .bind(note_id)
            .execute(&mut *tx)
            .await
            .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Failed to clear chunks".into()))?;

        let mut inserted_chunks = Vec::new();
        let mut reassembled_text = String::new();

        for (idx, ch) in chunks.into_iter().enumerate() {
            let chunk_index = ch.chunk_index.unwrap_or(idx as u32);
            let chunk_type = ch.chunk_type.unwrap_or_else(|| detect_chunk_type(&ch.content));
            let byte_size = ch.content.len() as u32;
            let checksum = compute_checksum(&ch.content);
            let chunk_uid = crate::chunker::generate_chunk_uid();

            reassembled_text.push_str(&ch.content);

            let res = sqlx::query(
                r#"
                INSERT INTO note_chunks
                    (note_id, chunk_uid, chunk_index, chunk_type, content, byte_size, checksum, created_at, updated_at)
                VALUES
                    (?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
                "#,
            )
            .bind(note_id)
            .bind(&chunk_uid)
            .bind(chunk_index)
            .bind(&chunk_type)
            .bind(&ch.content)
            .bind(byte_size)
            .bind(&checksum)
            .execute(&mut *tx)
            .await
            .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Chunk insert failed".into()))?;

            inserted_chunks.push(NoteChunk {
                id: res.last_insert_id(),
                note_id,
                chunk_uid,
                chunk_index,
                chunk_type,
                content: ch.content,
                byte_size,
                checksum: Some(checksum),
                metadata: None,
                created_at: Some(chrono::Utc::now()),
                updated_at: Some(chrono::Utc::now()),
            });
        }

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

        tx.commit().await.map_err(|_| {
            ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Commit failed".into())
        })?;

        state.cache.invalidate_note(user.id, note_id).await;

        return Ok(Json(serde_json::json!({
            "status": "success",
            "message": "Chunks successfully replaced!",
            "note_id": note_id,
            "chunks": inserted_chunks
        })).into_response());
    }

    // Case C: Provided single chunk_id with content
    if let (Some(cid), Some(ctype)) = (payload.chunk_id, payload.chunk_type) {
        let dummy_payload = UpdateChunkPayload {
            content: ctype,
            chunk_type: None,
        };
        let res = update_single_chunk(State(state), user, Path((note_id, cid)), Json(dummy_payload)).await?;
        return Ok(res.into_response());
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
    // 1. Verify note ownership
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

    // Invalidate Moka cache
    state.cache.invalidate_note(user.id, note_id).await;

    let updated_chunk = all_chunks.into_iter().find(|c| c.id == chunk_id);

    Ok(Json(serde_json::json!({
        "status": "success",
        "message": format!("Shard {} updated & checksum recomputed!", chunk_id),
        "data": {
            "chunk": updated_chunk,
            "note_id": note_id,
        }
    })).into_response())
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
