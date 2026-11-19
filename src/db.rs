use crate::config::Config;
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};
use std::time::Duration;

fn batch_end(drafts: &[crate::chunker::ChunkDraft], start: usize) -> usize {
    let mut end = start;
    let mut bytes = 0usize;
    while end < drafts.len() && end - start < 80 {
        let size = drafts[end].content.len()
            .saturating_add(drafts[end].metadata.as_ref().map_or(0, |v| v.to_string().len()));
        if end > start && bytes.saturating_add(size) > 512 * 1024 {
            break;
        }
        bytes = bytes.saturating_add(size);
        end += 1;
    }
    end
}

/// Insert bounded batches, then read actual IDs rather than assuming MySQL's
/// auto-increment values are contiguous.
pub async fn insert_chunk_drafts(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    note_id: u64,
    drafts: &[crate::chunker::ChunkDraft],
) -> Result<Vec<crate::models::NoteChunk>, sqlx::Error> {
    let mut start = 0;
    while start < drafts.len() {
        let end = batch_end(drafts, start);
        let mut builder = sqlx::QueryBuilder::<sqlx::MySql>::new(
            "INSERT INTO note_chunks (note_id, chunk_uid, chunk_index, chunk_type, content, byte_size, checksum, metadata, created_at, updated_at) "
        );
        builder.push_values(&drafts[start..end], |mut row, chunk| {
            row.push_bind(note_id)
                .push_bind(&chunk.chunk_uid)
                .push_bind(chunk.chunk_index)
                .push_bind(&chunk.chunk_type)
                .push_bind(&chunk.content)
                .push_bind(chunk.byte_size)
                .push_bind(&chunk.checksum)
                .push_bind(chunk.metadata.as_ref().map(|v| v.to_string()))
                .push("NOW()")
                .push("NOW()");
        });
        builder.build().execute(&mut **tx).await?;
        start = end;
    }
    sqlx::query_as::<_, crate::models::NoteChunk>(
        "SELECT id, note_id, chunk_uid, chunk_index, chunk_type, content, byte_size, checksum, metadata, created_at, updated_at FROM note_chunks WHERE note_id = ? ORDER BY chunk_index ASC, id ASC"
    ).bind(note_id).fetch_all(&mut **tx).await
}

#[cfg(test)]
mod tests {
    use super::batch_end;
    use crate::chunker::split_into_chunks;

    #[test]
    fn chunk_batches_limit_rows_and_bytes_without_stalling() {
        let drafts = split_into_chunks(&"a".repeat(241_000));
        let mut start = 0;
        let mut batches = 0;
        while start < drafts.len() {
            let end = batch_end(&drafts, start);
            assert!(end > start && end - start <= 80);
            batches += 1;
            start = end;
        }
        assert_eq!(batches, 4);
        let large = split_into_chunks(&format!("<p>{}</p><p>next</p>", "x".repeat(600_000)));
        assert_eq!(batch_end(&large, 0), 1);
    }
}

/// Initialize MySQL connection pool strictly configured for shared hosting safety:
/// - max_connections: 5 (prevent hitting shared hosting limits)
/// - min_connections: 1
/// - acquire_timeout: 5 seconds
pub async fn init_pool(config: &Config) -> Result<MySqlPool, sqlx::Error> {
    tracing::info!(
        "Initializing MySQL connection pool (max: {}, min: {}, acquire timeout: {}s)...",
        config.max_db_connections,
        config.min_db_connections,
        config.acquire_timeout_secs
    );

    let pool = MySqlPoolOptions::new()
        .max_connections(config.max_db_connections)
        .min_connections(config.min_db_connections)
        .acquire_timeout(Duration::from_secs(config.acquire_timeout_secs))
        .idle_timeout(Duration::from_secs(300))
        .connect(&config.database_url)
        .await?;

    tracing::info!("MySQL connection pool initialized successfully.");
    Ok(pool)
}
