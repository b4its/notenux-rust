use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct NoteChunk {
    pub id: u64,
    pub note_id: u64,
    pub chunk_uid: String,
    pub chunk_index: u32,
    pub chunk_type: String,
    pub content: String,
    pub byte_size: u32,
    pub checksum: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
pub struct ChunksQuery {
    pub offset: Option<u32>,
    pub limit: Option<u32>,
    pub after_index: Option<u32>,
    pub after_id: Option<u64>,
    pub lightweight: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateChunkPayload {
    pub content: String,
    pub chunk_type: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateChunksPayload {
    pub content: Option<String>,
    pub chunks: Option<Vec<ChunkInput>>,
    pub chunk_id: Option<u64>,
    pub chunk_type: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ChunkInput {
    pub id: Option<u64>,
    pub chunk_index: Option<u32>,
    pub chunk_type: Option<String>,
    pub content: String,
}

#[derive(Debug, Deserialize)]
pub struct ReorderChunksPayload {
    pub order: Vec<u64>,
}
