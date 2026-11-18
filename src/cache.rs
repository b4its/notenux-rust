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

#[derive(Debug, Clone)]
pub struct CachedPage {
    pub offset: u32,
    pub limit: u32,
    pub chunks: Vec<NoteChunk>,
}

impl CachedNoteData {
    pub fn new(note: Note, chunks: Vec<NoteChunk>) -> Self {
        let byte_size = weight(&note).saturating_add(weight(&chunks));

        Self {
            note,
            chunks,
            byte_size: byte_size.max(64),
        }
    }
}

fn compute_chunks_byte_size(chunks: &[NoteChunk]) -> u32 {
    weight(chunks)
}

fn compute_summaries_byte_size(summaries: &[NoteSummary]) -> u32 {
    weight(summaries)
}

fn weight<T: serde::Serialize + ?Sized>(value: &T) -> u32 {
    let bytes = serde_json::to_vec(value).map_or(u32::MAX as usize, |v| v.len());
    u32::try_from(bytes.saturating_mul(2).saturating_add(256)).unwrap_or(u32::MAX)
}

#[derive(Clone)]
pub struct AppCache {
    // Auth cache: token_key -> user_id (TTL: 10 minutes)
    auth: Cache<String, (u64, Option<chrono::DateTime<chrono::Utc>>)>,
    // Full note chunks cache: (user_id, note_id) -> CachedNoteData (TTL: 30 minutes, max 45 MB)
    notes: Cache<(u64, u64), Arc<CachedNoteData>>,
    // Dedicated cache for initial chunks (index 0 and 1) for instant top-of-note loading
    initial_chunks: Cache<(u64, u64), Arc<Vec<NoteChunk>>>,
    pages: Cache<(u64, u64), Arc<CachedPage>>,
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
            .max_capacity(max_memory_bytes / 4)
            .weigher(|_key: &(u64, u64), val: &Arc<CachedNoteData>| -> u32 { val.byte_size })
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

        let pages = Cache::builder()
            .max_capacity(max_memory_bytes / 4)
            .weigher(|_: &(u64, u64), page: &Arc<CachedPage>| {
                compute_chunks_byte_size(&page.chunks)
            })
            .time_to_live(Duration::from_secs(1800))
            .build();

        Self {
            auth,
            notes,
            initial_chunks,
            pages,
            notes_list,
        }
    }

    // --- Auth Cache (10 min TTL) ---
    pub async fn get_cached_user(&self, token_key: &str) -> Option<u64> {
        let (user_id, expires_at) = self.auth.get(token_key).await?;
        if expires_at.is_some_and(|expiry| expiry <= chrono::Utc::now()) {
            self.auth.invalidate(token_key).await;
            return None;
        }
        Some(user_id)
    }

    pub async fn cache_user(
        &self,
        token_key: String,
        user_id: u64,
        expires_at: Option<chrono::DateTime<chrono::Utc>>,
    ) {
        self.auth.insert(token_key, (user_id, expires_at)).await;
    }

    // --- Notes & Chunks Cache (30 min TTL) ---
    pub async fn get_note(&self, user_id: u64, note_id: u64) -> Option<Arc<CachedNoteData>> {
        self.notes.get(&(user_id, note_id)).await
    }

    pub async fn insert_note(&self, user_id: u64, note: Note, chunks: Vec<NoteChunk>) {
        let note_id = note.id;
        self.pages.invalidate(&(user_id, note_id)).await;
        self.initial_chunks.invalidate(&(user_id, note_id)).await;

        let detail = Arc::new(CachedNoteData::new(note, chunks));
        self.notes.insert((user_id, note_id), detail).await;

        // Invalidate list of notes so new or updated note is reflected
        self.notes_list.remove(&user_id).await;
    }

    // --- Dedicated Initial Chunks (chunk_index 0 and 1) ---
    pub async fn get_initial_chunks(
        &self,
        user_id: u64,
        note_id: u64,
    ) -> Option<Arc<Vec<NoteChunk>>> {
        self.initial_chunks.get(&(user_id, note_id)).await
    }

    pub async fn insert_initial_chunks(&self, user_id: u64, note_id: u64, chunks: Vec<NoteChunk>) {
        self.initial_chunks
            .insert((user_id, note_id), Arc::new(chunks))
            .await;
    }

    pub async fn get_page(
        &self,
        user_id: u64,
        note_id: u64,
        offset: u32,
        limit: u32,
    ) -> Option<Arc<CachedPage>> {
        self.pages
            .get(&(user_id, note_id))
            .await
            .filter(|p| p.offset == offset && p.limit == limit)
    }

    pub async fn insert_page(&self, user_id: u64, note_id: u64, page: CachedPage) {
        self.pages.insert((user_id, note_id), Arc::new(page)).await;
    }

    // --- Notes List Metadata Cache ---
    pub async fn get_notes_list(&self, user_id: u64) -> Option<Arc<Vec<NoteSummary>>> {
        self.notes_list.get(&user_id).await
    }

    pub async fn insert_notes_list(&self, user_id: u64, summaries: Vec<NoteSummary>) {
        self.notes_list.insert(user_id, Arc::new(summaries)).await;
    }

    // --- Cache Invalidation on Update / Delete ---
    pub async fn invalidate_note(&self, user_id: u64, note_id: u64) {
        self.notes.remove(&(user_id, note_id)).await;
        self.initial_chunks.remove(&(user_id, note_id)).await;
        self.pages.remove(&(user_id, note_id)).await;
        self.notes_list.remove(&user_id).await;
    }

    pub async fn invalidate_user_notes(&self, user_id: u64) {
        self.notes_list.remove(&user_id).await;
    }
}
