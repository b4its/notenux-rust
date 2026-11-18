use crate::models::{Note, NoteChunk, NoteSummary};
use moka::future::Cache;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct CachedNoteData {
    pub note: Note,
    pub chunks: Vec<NoteChunk>,
    pub byte_size: u32,
}

impl CachedNoteData {
    pub fn new(note: Note, chunks: Vec<NoteChunk>) -> Self {
        let mut byte_size = note.title.len() as u32 + note.category.len() as u32;
        if let Some(ref c) = note.content {
            byte_size += c.len() as u32;
        }
        for chunk in &chunks {
            byte_size += chunk.content.len() as u32;
            byte_size += chunk.chunk_uid.len() as u32;
            if let Some(ref cs) = chunk.checksum {
                byte_size += cs.len() as u32;
            }
        }

        Self {
            note,
            chunks,
            byte_size: byte_size.max(64),
        }
    }
}

fn compute_chunks_byte_size(chunks: &[NoteChunk]) -> u32 {
    let mut sum: u32 = 0;
    for ch in chunks {
        sum += ch.content.len() as u32 + ch.chunk_uid.len() as u32 + 32;
    }
    sum.max(64)
}

fn compute_summaries_byte_size(summaries: &[NoteSummary]) -> u32 {
    let mut sum: u32 = 0;
    for s in summaries {
        sum += s.title.len() as u32 + s.category.len() as u32 + 128;
    }
    sum.max(64)
}

#[derive(Clone)]
pub struct AppCache {
    // Auth cache: token_key -> user_id (TTL: 10 minutes)
    auth: Cache<String, u64>,
    // Full note chunks cache: (user_id, note_id) -> CachedNoteData (TTL: 30 minutes, max 45 MB)
    notes: Cache<(u64, u64), Arc<CachedNoteData>>,
    // Dedicated cache for initial chunks (index 0 and 1) for instant top-of-note loading
    initial_chunks: Cache<(u64, u64), Arc<Vec<NoteChunk>>>,
    // Notes list metadata cache: user_id -> Vec<NoteSummary> (TTL: 30 minutes)
    notes_list: Cache<u64, Arc<Vec<NoteSummary>>>,
}

impl AppCache {
    pub fn new(max_memory_bytes: u64) -> Self {
        // Auth cache: 10 minutes TTL
        let auth = Cache::builder()
            .max_capacity(5_000)
            .time_to_live(Duration::from_secs(600)) // 10 minutes
            .build();

        // 45 MB bounded memory cache for note data with 30 min TTL
        let notes = Cache::builder()
            .max_capacity(max_memory_bytes)
            .weigher(|_key: &(u64, u64), val: &Arc<CachedNoteData>| -> u32 {
                val.byte_size
            })
            .time_to_live(Duration::from_secs(1800)) // 30 minutes
            .time_to_idle(Duration::from_secs(900))
            .build();

        // Dedicated cache prioritizing initial chunks (0 and 1)
        let initial_chunks = Cache::builder()
            .max_capacity(max_memory_bytes / 4)
            .weigher(|_key: &(u64, u64), val: &Arc<Vec<NoteChunk>>| -> u32 {
                compute_chunks_byte_size(val)
            })
            .time_to_live(Duration::from_secs(1800)) // 30 minutes
            .build();

        // Notes list metadata cache
        let notes_list = Cache::builder()
            .max_capacity(max_memory_bytes / 4)
            .weigher(|_key: &u64, val: &Arc<Vec<NoteSummary>>| -> u32 {
                compute_summaries_byte_size(val)
            })
            .time_to_live(Duration::from_secs(1800)) // 30 minutes
            .build();

        Self {
            auth,
            notes,
            initial_chunks,
            notes_list,
        }
    }

    // --- Auth Cache (10 min TTL) ---
    pub async fn get_cached_user(&self, token_key: &str) -> Option<u64> {
        self.auth.get(token_key).await
    }

    pub async fn cache_user(&self, token_key: String, user_id: u64) {
        self.auth.insert(token_key, user_id).await;
    }

    // --- Notes & Chunks Cache (30 min TTL) ---
    pub async fn get_note(&self, user_id: u64, note_id: u64) -> Option<Arc<CachedNoteData>> {
        self.notes.get(&(user_id, note_id)).await
    }

    pub async fn insert_note(&self, user_id: u64, note: Note, chunks: Vec<NoteChunk>) {
        let note_id = note.id;

        // Cache initial chunks (chunk_index 0 and 1) with high priority
        let initials: Vec<NoteChunk> = chunks
            .iter()
            .filter(|c| c.chunk_index <= 1)
            .cloned()
            .collect();
        if !initials.is_empty() {
            self.initial_chunks
                .insert((user_id, note_id), Arc::new(initials))
                .await;
        }

        let detail = Arc::new(CachedNoteData::new(note, chunks));
        self.notes.insert((user_id, note_id), detail).await;

        // Invalidate list of notes so new or updated note is reflected
        self.notes_list.remove(&user_id).await;
    }

    // --- Dedicated Initial Chunks (chunk_index 0 and 1) ---
    pub async fn get_initial_chunks(&self, user_id: u64, note_id: u64) -> Option<Arc<Vec<NoteChunk>>> {
        self.initial_chunks.get(&(user_id, note_id)).await
    }

    pub async fn insert_initial_chunks(&self, user_id: u64, note_id: u64, chunks: Vec<NoteChunk>) {
        self.initial_chunks
            .insert((user_id, note_id), Arc::new(chunks))
            .await;
    }

    // --- Notes List Metadata Cache ---
    pub async fn get_notes_list(&self, user_id: u64) -> Option<Arc<Vec<NoteSummary>>> {
        self.notes_list.get(&user_id).await
    }

    pub async fn insert_notes_list(&self, user_id: u64, summaries: Vec<NoteSummary>) {
        self.notes_list
            .insert(user_id, Arc::new(summaries))
            .await;
    }

    // --- Cache Invalidation on Update / Delete ---
    pub async fn invalidate_note(&self, user_id: u64, note_id: u64) {
        self.notes.remove(&(user_id, note_id)).await;
        self.initial_chunks.remove(&(user_id, note_id)).await;
        self.notes_list.remove(&user_id).await;
    }

    pub async fn invalidate_user_notes(&self, user_id: u64) {
        self.notes_list.remove(&user_id).await;
    }
}
