use crate::models::{Note, NoteChunk};
use moka::future::Cache;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct CachedNoteDetail {
    pub note: Note,
    pub chunks: Vec<NoteChunk>,
    pub estimated_bytes: u32,
}

impl CachedNoteDetail {
    pub fn new(note: Note, chunks: Vec<NoteChunk>) -> Self {
        let mut bytes = std::mem::size_of::<Note>() as u32;
        bytes += note.title.len() as u32;
        bytes += note.category.len() as u32;
        if let Some(ref c) = note.content {
            bytes += c.len() as u32;
        }

        bytes += (chunks.len() * std::mem::size_of::<NoteChunk>()) as u32;
        for ch in &chunks {
            bytes += ch.chunk_uid.len() as u32;
            bytes += ch.content.len() as u32;
            if let Some(ref cs) = ch.checksum {
                bytes += cs.len() as u32;
            }
        }

        Self {
            note,
            chunks,
            estimated_bytes: bytes.max(64),
        }
    }
}

#[derive(Clone)]
pub struct AppCache {
    // Key: (user_id, note_id)
    notes: Cache<(u64, u64), Arc<CachedNoteDetail>>,
    // Auth cache: token_hash -> user_id (TTL 60s)
    auth: Cache<String, u64>,
}

impl AppCache {
    pub fn new(max_memory_bytes: u64) -> Self {
        let notes = Cache::builder()
            .max_capacity(max_memory_bytes)
            .weigher(|_key: &(u64, u64), val: &Arc<CachedNoteDetail>| -> u32 {
                val.estimated_bytes
            })
            .time_to_live(Duration::from_secs(300))
            .time_to_idle(Duration::from_secs(120))
            .build();

        let auth = Cache::builder()
            .max_capacity(2000)
            .time_to_live(Duration::from_secs(60))
            .build();

        Self { notes, auth }
    }

    pub async fn get_note(&self, user_id: u64, note_id: u64) -> Option<Arc<CachedNoteDetail>> {
        self.notes.get(&(user_id, note_id)).await
    }

    pub async fn insert_note(&self, user_id: u64, note: Note, chunks: Vec<NoteChunk>) {
        let note_id = note.id;
        let detail = Arc::new(CachedNoteDetail::new(note, chunks));
        self.notes.insert((user_id, note_id), detail).await;
    }

    pub async fn invalidate_note(&self, user_id: u64, note_id: u64) {
        self.notes.remove(&(user_id, note_id)).await;
    }

    pub async fn get_cached_user(&self, token_hash: &str) -> Option<u64> {
        self.auth.get(token_hash).await
    }

    pub async fn cache_user(&self, token_hash: String, user_id: u64) {
        self.auth.insert(token_hash, user_id).await;
    }
}
