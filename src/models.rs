use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Note {
    pub id: u64,
    pub user_id: u64,
    pub folder_id: Option<u64>,
    pub uuid: String,
    pub title: String,
    pub category: String,
    pub content: Option<String>,
    pub font_family: String,
    pub font_size: String,
    pub theme_color: String,
    pub word_count: u32,
    pub char_count: u32,
    pub is_pinned: bool,
    pub is_favorite: bool,
    pub level: u32,
    pub xp: u32,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

impl Note {
    pub fn reading_time(&self) -> String {
        let words = self.word_count;
        let mins = if words == 0 {
            1
        } else {
            (words as f64 / 180.0).ceil() as u32
        };
        format!("{} min read", mins)
    }
}

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoteSummary {
    pub id: u64,
    pub user_id: u64,
    pub folder_id: Option<u64>,
    pub uuid: String,
    pub title: String,
    pub category: String,
    pub font_family: String,
    pub font_size: String,
    pub theme_color: String,
    pub word_count: u32,
    pub char_count: u32,
    pub is_pinned: bool,
    pub is_favorite: bool,
    pub level: u32,
    pub xp: u32,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
    pub reading_time: String,
    pub chunks_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoteWithChunks {
    #[serde(flatten)]
    pub note: Note,
    pub reading_time: String,
    pub chunks: Vec<NoteChunk>,
}

#[derive(Debug, Deserialize)]
pub struct CreateNotePayload {
    pub title: Option<String>,
    pub category: Option<String>,
    pub folder_id: Option<u64>,
    pub content: Option<String>,
    pub font_family: Option<String>,
    pub font_size: Option<String>,
    pub theme_color: Option<String>,
    pub is_pinned: Option<bool>,
    pub is_favorite: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateNotePayload {
    pub title: Option<String>,
    pub category: Option<String>,
    pub folder_id: Option<u64>,
    pub content: Option<String>,
    pub font_family: Option<String>,
    pub font_size: Option<String>,
    pub theme_color: Option<String>,
    pub is_pinned: Option<bool>,
    pub is_favorite: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateChunkPayload {
    pub content: String,
    pub chunk_type: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ReorderChunksPayload {
    pub order: Vec<u64>,
}

#[derive(Debug, Deserialize)]
pub struct ChunksQuery {
    pub offset: Option<u32>,
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct NotesListQuery {
    pub q: Option<String>,
    pub category: Option<String>,
    pub folder_id: Option<u64>,
    pub sort: Option<String>,
    pub pinned_only: Option<bool>,
    pub favorites_only: Option<bool>,
}
