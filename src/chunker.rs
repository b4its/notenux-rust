use regex::Regex;
use sha2::{Digest, Sha256};
use std::sync::LazyLock;

pub const MAX_CHUNK_SIZE: usize = 1000;

static HTML_TAG_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?s)<!--.*?-->|</?[a-zA-Z][^>]*>|[^<]+"#).unwrap()
});

static TAG_NAME_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"^</?([a-zA-Z0-9]+)"#).unwrap()
});

static WORD_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"[\p{L}\p{N}]+(?:['’\-][\p{L}\p{N}]+)*"#).unwrap()
});

static HTML_STRIP_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"<[^>]+>"#).unwrap()
});

static BLOCK_END_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?i)</(?:p|div|h[1-6]|li|tr|blockquote)>|<br\s*/?>"#).unwrap()
});

#[derive(Debug, Clone)]
pub struct ChunkDraft {
    pub chunk_uid: String,
    pub chunk_index: u32,
    pub chunk_type: String,
    pub content: String,
    pub byte_size: u32,
    pub checksum: String,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Copy)]
pub struct NoteMetrics {
    pub word_count: u32,
    pub char_count: u32,
    pub xp: u32,
    pub level: u32,
}

pub fn detect_chunk_type(content: &str) -> String {
    let trimmed = content.trim();
    if trimmed.is_empty() {
        return "paragraph".to_string();
    }

    let lower = trimmed.to_lowercase();
    if lower.starts_with("<pre") || lower.starts_with("<code") || trimmed.starts_with("```") {
        return "code".to_string();
    }
    if lower.starts_with("<blockquote") || trimmed.starts_with('>') {
        return "quote".to_string();
    }
    if lower.starts_with("<table") || trimmed.starts_with('|') {
        return "table".to_string();
    }
    if trimmed.starts_with("- [ ]")
        || trimmed.starts_with("- [x]")
        || trimmed.starts_with("- [X]")
        || trimmed.starts_with("* [ ]")
        || trimmed.starts_with("* [x]")
        || lower.contains("class=\"task")
        || lower.contains("type=\"checkbox\"")
    {
        return "task".to_string();
    }
    if lower.starts_with("<ul")
        || lower.starts_with("<ol")
        || lower.starts_with("<li")
        || trimmed.starts_with("- ")
        || trimmed.starts_with("* ")
        || trimmed.starts_with("+ ")
        || trimmed.chars().next().is_some_and(|c| c.is_ascii_digit())
            && (trimmed.contains(". ") || trimmed.contains(".\t"))
    {
        return "list".to_string();
    }

    "paragraph".to_string()
}

pub fn compute_checksum(content: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    hex::encode(hasher.finalize())
}

pub fn generate_chunk_uid() -> String {
    let raw = uuid::Uuid::new_v4().simple().to_string();
    format!("chk_{}", raw)
}

pub fn split_into_chunks(content: &str) -> Vec<ChunkDraft> {
    if content.is_empty() {
        let checksum = compute_checksum("");
        return vec![ChunkDraft {
            chunk_uid: generate_chunk_uid(),
            chunk_index: 0,
            chunk_type: "paragraph".to_string(),
            content: String::new(),
            byte_size: 0,
            checksum,
            metadata: Some(serde_json::json!({ "empty": true })),
        }];
    }

    let is_html = content.contains('<') && content.contains('>');
    let raw_blocks: Vec<String> = if is_html {
        split_html_blocks(content)
    } else {
        split_plain_text_blocks(content)
    };

    raw_blocks
        .into_iter()
        .enumerate()
        .map(|(index, block)| {
            let chunk_type = detect_chunk_type(&block);
            let byte_size = block.len() as u32;
            let checksum = compute_checksum(&block);
            ChunkDraft {
                chunk_uid: generate_chunk_uid(),
                chunk_index: index as u32,
                chunk_type,
                content: block,
                byte_size,
                checksum,
                metadata: None,
            }
        })
        .collect()
}

fn split_html_blocks(content: &str) -> Vec<String> {
    let void_tags = [
        "area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param",
        "source", "track", "wbr",
    ];

    let mut raw_blocks = Vec::new();
    let mut current_accumulator = String::new();
    let mut current_block = String::new();
    let mut open_tags: Vec<String> = Vec::new();

    for mat in HTML_TAG_REGEX.find_iter(content) {
        let part = mat.as_str();
        current_block.push_str(part);

        if part.starts_with("</") {
            if let Some(captures) = TAG_NAME_REGEX.captures(part) {
                let tag_name = captures[1].to_lowercase();
                if open_tags.last().map(|s| s.as_str()) == Some(&tag_name) {
                    open_tags.pop();
                }
            }
        } else if part.starts_with('<') && !part.starts_with("<!--") && !part.ends_with("/>") {
            if let Some(captures) = TAG_NAME_REGEX.captures(part) {
                let tag_name = captures[1].to_lowercase();
                if !void_tags.contains(&tag_name.as_str()) {
                    open_tags.push(tag_name);
                }
            }
        }

        if open_tags.is_empty() {
            if !current_accumulator.is_empty()
                && (current_accumulator.len() + current_block.len() > MAX_CHUNK_SIZE)
            {
                raw_blocks.push(std::mem::take(&mut current_accumulator));
            }
            current_accumulator.push_str(&current_block);
            current_block.clear();
        }
    }

    current_accumulator.push_str(&current_block);
    if !current_accumulator.is_empty() {
        raw_blocks.push(current_accumulator);
    }

    if raw_blocks.is_empty() {
        vec![content.to_string()]
    } else {
        raw_blocks
    }
}

fn split_plain_text_blocks(content: &str) -> Vec<String> {
    let mut raw_blocks = Vec::new();
    let mut start = 0;
    while start < content.len() {
        let mut end = (start + MAX_CHUNK_SIZE).min(content.len());
        while !content.is_char_boundary(end) {
            end -= 1;
        }
        raw_blocks.push(content[start..end].to_string());
        start = end;
    }
    raw_blocks
}

pub fn calculate_metrics(content: &str) -> NoteMetrics {
    let spaced = BLOCK_END_REGEX.replace_all(content, " ");
    let stripped = HTML_STRIP_REGEX.replace_all(&spaced, "");
    let decoded = decode_html_entities(&stripped);
    let clean_text = decoded.trim();

    let word_count = WORD_REGEX.find_iter(clean_text).count() as u32;
    let char_count = clean_text.chars().count() as u32;

    let xp = std::cmp::max(10, ((word_count as f64) * 3.0).round() as u32);
    let level = std::cmp::max(1, (xp / 120) + 1);

    NoteMetrics {
        word_count,
        char_count,
        xp,
        level,
    }
}

fn decode_html_entities(s: &str) -> String {
    s.replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#039;", "'")
        .replace("&apos;", "'")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_empty_content() {
        let chunks = split_into_chunks("");
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].chunk_index, 0);
        assert_eq!(chunks[0].content, "");
        assert_eq!(chunks[0].byte_size, 0);
    }

    #[test]
    fn test_split_small_content() {
        let content = "<p>Halo Dunia</p>";
        let chunks = split_into_chunks(content);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].content, content);
        assert_eq!(chunks[0].chunk_type, "paragraph");
        assert_eq!(chunks[0].byte_size, content.len() as u32);
        assert_eq!(chunks[0].checksum, compute_checksum(content));
    }

    #[test]
    fn test_metrics_calculation() {
        let content = "<p>Halo dari <strong>Notenux</strong>!</p>";
        let metrics = calculate_metrics(content);
        assert_eq!(metrics.word_count, 3);
        assert!(metrics.char_count > 0);
        assert_eq!(metrics.xp, 10);
        assert_eq!(metrics.level, 1);
    }

    #[test]
    fn large_plain_text_round_trips_without_losing_separators_or_unicode() {
        let content = format!("{}\n\n{}  ", "a".repeat(995), "🦀".repeat(2000));
        let chunks = split_into_chunks(&content);
        assert!(chunks.len() > 2);
        assert!(chunks.iter().all(|chunk| chunk.content.len() <= MAX_CHUNK_SIZE));
        assert_eq!(chunks.iter().map(|chunk| chunk.content.as_str()).collect::<String>(), content);
        assert_eq!(split_into_chunks("  \n")[0].content, "  \n");
    }
}
