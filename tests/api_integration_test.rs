use axum::{
    body::Body,
    http::{header, Request, StatusCode},
};
use be_notenux::{config::Config, create_app};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

#[tokio::test]
async fn test_health_check() {
    let config = Config::from_env();
    let app = create_app(&config).await.expect("Failed to create app");

    let response = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["status"], "healthy");
    assert_eq!(json["service"], "be-notenux");
}

#[tokio::test]
async fn test_unauthenticated_request_rejected() {
    let config = Config::from_env();
    let app = create_app(&config).await.expect("Failed to create app");

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/notes")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_dev_header_and_malformed_tokens_cannot_bypass_authentication() {
    let app = create_app(&Config::from_env())
        .await
        .expect("Failed to create app");
    for token in [None, Some("Bearer not-a-sanctum-token"), Some("Bearer 1|")] {
        let mut request = Request::builder()
            .uri("/api/notes")
            .header("x-dev-user-id", "1");
        if let Some(token) = token {
            request = request.header(header::AUTHORIZATION, token);
        }
        let response = app
            .clone()
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}

#[tokio::test]
async fn large_writes_are_bounded_without_blocking_reads() {
    use std::{convert::Infallible, time::Duration};
    let app = create_app(&Config::from_env()).await.unwrap();
    let mut tasks = Vec::new();
    for _ in 0..2 {
        let (started, receiver) = tokio::sync::oneshot::channel();
        let body = Body::from_stream(futures_util::stream::once(async move {
            let _ = started.send(());
            futures_util::future::pending::<Result<axum::body::Bytes, Infallible>>().await
        }));
        let request = Request::builder().method("POST").uri("/api/notes")
            .header(header::AUTHORIZATION, "Bearer 1|sanctum_test_token_1234567890abcdef")
            .header(header::CONTENT_TYPE, "application/json").body(body).unwrap();
        tasks.push(tokio::spawn(app.clone().oneshot(request)));
        tokio::time::timeout(Duration::from_secs(5), receiver).await.unwrap().unwrap();
    }
    let (started, receiver) = tokio::sync::oneshot::channel();
    let body = Body::from_stream(futures_util::stream::once(async move {
        let _ = started.send(());
        futures_util::future::pending::<Result<axum::body::Bytes, Infallible>>().await
    }));
    let request = Request::builder().method("PUT").uri("/api/notes/1/chunks/1")
        .header(header::AUTHORIZATION, "Bearer 1|sanctum_test_token_1234567890abcdef")
        .header(header::CONTENT_TYPE, "application/json").body(body).unwrap();
    tasks.push(tokio::spawn(app.clone().oneshot(request)));
    assert!(tokio::time::timeout(Duration::from_millis(100), receiver).await.is_err());
    let read = Request::builder().uri("/api/notes")
        .header(header::AUTHORIZATION, "Bearer 1|sanctum_test_token_1234567890abcdef")
        .body(Body::empty()).unwrap();
    let response = tokio::time::timeout(Duration::from_secs(5), app.oneshot(read)).await.unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    for task in tasks { task.abort(); }
}

#[tokio::test]
async fn oversized_note_write_is_rejected() {
    let app = create_app(&Config::from_env()).await.unwrap();
    let request = Request::builder().method("POST").uri("/api/notes")
        .header(header::AUTHORIZATION, "Bearer 1|sanctum_test_token_1234567890abcdef")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(format!(r#"{{"content":"{}"}}"#, "x".repeat(8 * 1024 * 1024))))
        .unwrap();
    assert_eq!(app.oneshot(request).await.unwrap().status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn test_invalid_sanctum_token_rejected() {
    let config = Config::from_env();
    let app = create_app(&config).await.expect("Failed to create app");

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/notes")
                .header(header::AUTHORIZATION, "Bearer 1|invalid_token_secret_xyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_cors_preflight_and_headers() {
    let config = Config::from_env();
    let app = create_app(&config).await.expect("Failed to create app");

    // Test preflight OPTIONS request from allowed origin http://localhost:5173
    let options_req = Request::builder()
        .method("OPTIONS")
        .uri("/api/notes")
        .header(header::ORIGIN, "http://localhost:5173")
        .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
        .header(
            header::ACCESS_CONTROL_REQUEST_HEADERS,
            "authorization,content-type",
        )
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(options_req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let allow_origin = response
        .headers()
        .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
        .unwrap();
    assert_eq!(allow_origin.to_str().unwrap(), "http://localhost:5173");

    // Test allowed origin https://app.notenux.oryphem.com
    let options_prod_req = Request::builder()
        .method("OPTIONS")
        .uri("/api/notes")
        .header(header::ORIGIN, "https://app.notenux.oryphem.com")
        .header(header::ACCESS_CONTROL_REQUEST_METHOD, "GET")
        .body(Body::empty())
        .unwrap();

    let prod_response = app.oneshot(options_prod_req).await.unwrap();
    assert_eq!(prod_response.status(), StatusCode::OK);
    let prod_allow_origin = prod_response
        .headers()
        .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
        .unwrap();
    assert_eq!(
        prod_allow_origin.to_str().unwrap(),
        "https://app.notenux.oryphem.com"
    );
}

#[tokio::test]
async fn test_full_specification_workflow() {
    let config = Config::from_env();
    let app = create_app(&config).await.expect("Failed to create app");

    // Laravel Sanctum Bearer token: {id}|{plain_text_token}
    let token = "1|sanctum_test_token_1234567890abcdef";

    // 1. POST /api/notes: Create note with multi-chunk content
    let block0 = format!(
        "<p>Chunk 0: Pembukaan misteri gulungan naga emas. {}</p>",
        "A".repeat(1100)
    );
    let block1 = format!(
        "<p>Chunk 1: Penjelasan mantra kuno elemen petir. {}</p>",
        "B".repeat(1100)
    );
    let block2 = format!(
        "<p>Chunk 2: Ritual rahasia para petapa gunung suci. {}</p>",
        "C".repeat(1100)
    );
    let block3 = format!(
        "<p>Chunk 3: Bab penutup prasasti kekekalan abadi. {}</p>",
        "D".repeat(1100)
    );
    let multi_chunk_content = format!("{}\n{}\n{}\n{}", block0, block1, block2, block3);

    let create_payload = serde_json::json!({
        "title": "Kitab Petapa Naga",
        "category": "catatan",
        "content": multi_chunk_content,
        "font_family": "pixel",
        "font_size": "16px",
        "theme_color": "green",
    });

    let create_req = Request::builder()
        .method("POST")
        .uri("/api/notes")
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(create_payload.to_string()))
        .unwrap();

    let create_resp = app.clone().oneshot(create_req).await.unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);

    let bytes = create_resp.into_body().collect().await.unwrap().to_bytes();
    let created_json: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(created_json["status"], "success");
    let note_id = created_json["data"]["id"].as_u64().unwrap();
    let chunks = created_json["data"]["chunks"].as_array().unwrap();
    assert!(chunks.len() >= 4);

    // 2. GET /api/notes: Verify metadata list does not load full content
    let list_req = Request::builder()
        .uri("/api/notes")
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();

    let list_resp = app.clone().oneshot(list_req).await.unwrap();
    assert_eq!(list_resp.status(), StatusCode::OK);
    let bytes = list_resp.into_body().collect().await.unwrap().to_bytes();
    let list_json: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(list_json["status"], "success");
    let note_summary = list_json["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|n| n["id"] == note_id)
        .expect("Created note should be in list");
    assert_eq!(note_summary["title"], "Kitab Petapa Naga");
    // Ensure metadata does not embed full content field
    assert!(note_summary.get("content").is_none());
    assert!(note_summary["chunks_count"].as_u64().unwrap() >= 4);

    // 3. GET /api/notes/{note_id}/chunks?offset=0&limit=2: fetch initial 2 chunks in ASC order
    let chunks_req = Request::builder()
        .uri(format!("/api/notes/{}/chunks?offset=0&limit=2", note_id))
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();

    let chunks_resp = app.clone().oneshot(chunks_req).await.unwrap();
    assert_eq!(chunks_resp.status(), StatusCode::OK);
    let bytes = chunks_resp.into_body().collect().await.unwrap().to_bytes();
    let chunks_json: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(chunks_json["status"], "success");
    assert_eq!(chunks_json["offset"], 0);
    assert_eq!(chunks_json["limit"], 2);
    let returned_chunks = chunks_json["chunks"].as_array().unwrap();
    assert_eq!(returned_chunks.len(), 2);
    // Verify ASC order
    assert_eq!(returned_chunks[0]["chunk_index"], 0);
    assert_eq!(returned_chunks[1]["chunk_index"], 1);
    assert_eq!(chunks_json["has_more"], true);

    let later_req = Request::builder()
        .uri(format!("/api/notes/{}/chunks?offset=2&limit=1", note_id))
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();
    let later_resp = app.clone().oneshot(later_req).await.unwrap();
    assert_eq!(later_resp.status(), StatusCode::OK);
    let later: Value =
        serde_json::from_slice(&later_resp.into_body().collect().await.unwrap().to_bytes())
            .unwrap();
    assert_eq!(later["chunks"].as_array().unwrap().len(), 1);
    assert_eq!(later["chunks"][0]["chunk_index"], 2);
    assert_eq!(later["total_bytes"], chunks_json["total_bytes"]);

    let cursor_id = returned_chunks[1]["id"].as_u64().unwrap();
    let cursor_req = Request::builder()
        .uri(format!("/api/notes/{note_id}/chunks?offset=0&limit=1&lightweight=true&after_index=1&after_id={cursor_id}"))
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .body(Body::empty()).unwrap();
    let cursor_response = app.clone().oneshot(cursor_req).await.unwrap();
    assert_eq!(cursor_response.status(), StatusCode::OK);
    let cursor_json: Value = serde_json::from_slice(&cursor_response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(cursor_json["chunks"][0]["chunk_index"], 2);
    assert_eq!(cursor_json["has_more"], true);
    assert!(cursor_json.get("total_chunks").is_none());
    let malformed_cursor = Request::builder()
        .uri(format!("/api/notes/{note_id}/chunks?lightweight=true&after_index=1"))
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .body(Body::empty()).unwrap();
    assert_eq!(app.clone().oneshot(malformed_cursor).await.unwrap().status(), StatusCode::BAD_REQUEST);

    // 4. Repeated chunk query hits initial chunks Moka cache
    let cache_hit_req = Request::builder()
        .uri(format!("/api/notes/{}/chunks?offset=0&limit=2", note_id))
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();
    let cache_hit_resp = app.clone().oneshot(cache_hit_req).await.unwrap();
    assert_eq!(cache_hit_resp.status(), StatusCode::OK);

    // 5. GET /api/notes/search?q=kata_kunci: Fast search by keyword in note / chunk
    let search_req = Request::builder()
        .uri("/api/notes/search?q=ritual+rahasia")
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();

    let search_resp = app.clone().oneshot(search_req).await.unwrap();
    assert_eq!(search_resp.status(), StatusCode::OK);
    let bytes = search_resp.into_body().collect().await.unwrap().to_bytes();
    let search_json: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(search_json["status"], "success");
    let search_results = search_json["data"].as_array().unwrap();
    assert!(search_results.iter().any(|item| item["id"] == note_id));

    // 6. PUT /api/notes/{note_id}/chunks: Update chunks and invalidate cache
    let up0 = format!(
        "<p>Chunk 0: Bab Baru Naga Berbisik. {}</p>",
        "X".repeat(1100)
    );
    let up1 = format!(
        "<p>Chunk 1: Petir menyambar langit kelam. {}</p>",
        "Y".repeat(1100)
    );
    let updated_content = format!("{}\n{}", up0, up1);

    let update_chunks_payload = serde_json::json!({
        "content": updated_content
    });

    let put_chunks_req = Request::builder()
        .method("PUT")
        .uri(format!("/api/notes/{}/chunks", note_id))
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(update_chunks_payload.to_string()))
        .unwrap();

    let put_chunks_resp = app.clone().oneshot(put_chunks_req).await.unwrap();
    assert_eq!(put_chunks_resp.status(), StatusCode::OK);

    // Verify updated chunks via GET
    let verify_get_req = Request::builder()
        .uri(format!("/api/notes/{}/chunks?offset=0&limit=2", note_id))
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();
    let verify_get_resp = app.clone().oneshot(verify_get_req).await.unwrap();
    assert_eq!(verify_get_resp.status(), StatusCode::OK);
    let bytes = verify_get_resp
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let verify_json: Value = serde_json::from_slice(&bytes).unwrap();
    let new_chunks = verify_json["chunks"].as_array().unwrap();
    assert_eq!(new_chunks.len(), 2);
    assert!(new_chunks[0]["content"]
        .as_str()
        .unwrap()
        .contains("Naga Berbisik"));

    // 7. GET /api/notes/{note_id}/chunks/verify: Verify cryptographic integrity
    let verify_checksum_req = Request::builder()
        .uri(format!("/api/notes/{}/chunks/verify", note_id))
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();
    let verify_checksum_resp = app.clone().oneshot(verify_checksum_req).await.unwrap();
    assert_eq!(verify_checksum_resp.status(), StatusCode::OK);
    let bytes = verify_checksum_resp
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let verify_checksum_json: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(verify_checksum_json["data"]["verified"], true);

    // 8. PUT /api/notes/{note_id}/chunks with explicit chunks array
    let chunk_array_payload = serde_json::json!({
        "chunks": [
            {
                "chunk_index": 0,
                "chunk_type": "paragraph",
                "content": "<p>Explicit Chunk Alpha</p>"
            },
            {
                "chunk_index": 1,
                "chunk_type": "code",
                "content": "<pre><code>let level = 99;</code></pre>"
            }
        ]
    });
    let put_array_req = Request::builder()
        .method("PUT")
        .uri(format!("/api/notes/{}/chunks", note_id))
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(chunk_array_payload.to_string()))
        .unwrap();
    let put_array_resp = app.clone().oneshot(put_array_req).await.unwrap();
    assert_eq!(put_array_resp.status(), StatusCode::OK);
    let payload: Value = serde_json::from_slice(&put_array_resp.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let first_id = payload["chunks"][0]["id"].as_u64().unwrap();
    let update_req = Request::builder()
        .method("PUT")
        .uri(format!("/api/notes/{note_id}/chunks/{first_id}"))
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"content":"<p>Updated chunk</p>"}"#)).unwrap();
    let response = app.clone().oneshot(update_req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let updated: Value = serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(updated["data"]["chunk"]["content"], "<p>Updated chunk</p>");
    let get_req = Request::builder().uri(format!("/api/notes/{note_id}"))
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty()).unwrap();
    let response = app.clone().oneshot(get_req).await.unwrap();
    let note: Value = serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(note["data"]["content"], "<p>Updated chunk</p><pre><code>let level = 99;</code></pre>");

    // 9. POST /api/notes/{note_id}/pin and /favorite
    let pin_req = Request::builder()
        .method("POST")
        .uri(format!("/api/notes/{}/pin", note_id))
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();
    let pin_resp = app.clone().oneshot(pin_req).await.unwrap();
    assert_eq!(pin_resp.status(), StatusCode::OK);

    let fav_req = Request::builder()
        .method("POST")
        .uri(format!("/api/notes/{}/favorite", note_id))
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();
    let fav_resp = app.clone().oneshot(fav_req).await.unwrap();
    assert_eq!(fav_resp.status(), StatusCode::OK);

    // 10. POST /api/notes/{note_id}/duplicate
    let dup_req = Request::builder()
        .method("POST")
        .uri(format!("/api/notes/{}/duplicate", note_id))
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();
    let dup_resp = app.clone().oneshot(dup_req).await.unwrap();
    assert_eq!(dup_resp.status(), StatusCode::CREATED);
    let bytes = dup_resp.into_body().collect().await.unwrap().to_bytes();
    let dup_json: Value = serde_json::from_slice(&bytes).unwrap();
    let dup_id = dup_json["data"]["id"].as_u64().unwrap();
    assert_ne!(dup_id, note_id);

    // 11. Clean up: DELETE cloned and original note
    let del_dup_req = Request::builder()
        .method("DELETE")
        .uri(format!("/api/notes/{}", dup_id))
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();
    let del_dup_resp = app.clone().oneshot(del_dup_req).await.unwrap();
    assert_eq!(del_dup_resp.status(), StatusCode::OK);

    let delete_req = Request::builder()
        .method("DELETE")
        .uri(format!("/api/notes/{}", note_id))
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();
    let delete_resp = app.clone().oneshot(delete_req).await.unwrap();
    assert_eq!(delete_resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_large_plain_note_cursor_round_trip() {
    let app = create_app(&Config::from_env()).await.expect("test database required");
    let token = "Bearer 1|sanctum_test_token_1234567890abcdef";
    let content = format!("{}🦀\n\n{}", "a".repeat(1_050_000), "b".repeat(1_050_000));
    let create = Request::builder().method("POST").uri("/api/notes")
        .header(header::AUTHORIZATION, token).header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::json!({"title":"Cursor performance fixture", "content":content}).to_string())).unwrap();
    let response = app.clone().oneshot(create).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let value: Value = serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let note_id = value["data"]["id"].as_u64().unwrap();
    let created_ids: Vec<u64> = value["data"]["chunks"].as_array().unwrap().iter()
        .map(|chunk| chunk["id"].as_u64().unwrap()).collect();
    let mut reconstructed = String::new();
    let mut loaded_ids = Vec::new();
    let mut cursor = String::new();
    let mut pages = 0;
    loop {
        let request = Request::builder().uri(format!("/api/notes/{note_id}/chunks?limit=50&lightweight=true{cursor}"))
            .header(header::AUTHORIZATION, token).body(Body::empty()).unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let page: Value = serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
        let chunks = page["chunks"].as_array().unwrap();
        for chunk in chunks {
            reconstructed.push_str(chunk["content"].as_str().unwrap());
            loaded_ids.push(chunk["id"].as_u64().unwrap());
        }
        pages += 1;
        if !page["has_more"].as_bool().unwrap() { break; }
        let last = chunks.last().unwrap();
        cursor = format!("&after_index={}&after_id={}", last["chunk_index"], last["id"]);
    }
    assert!(pages > 1);
    assert_eq!(reconstructed, content);
    assert_eq!(loaded_ids, created_ids);
    let update_note = Request::builder().method("PUT").uri(format!("/api/notes/{note_id}"))
        .header(header::AUTHORIZATION, token).header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::json!({"content": content}).to_string())).unwrap();
    assert_eq!(app.clone().oneshot(update_note).await.unwrap().status(), StatusCode::OK);
    let update_all_chunks = Request::builder().method("PUT").uri(format!("/api/notes/{note_id}/chunks"))
        .header(header::AUTHORIZATION, token).header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::json!({"content": content}).to_string())).unwrap();
    assert_eq!(app.clone().oneshot(update_all_chunks).await.unwrap().status(), StatusCode::OK);
    let replace = Request::builder().method("PUT").uri(format!("/api/notes/{note_id}/chunks"))
        .header(header::AUTHORIZATION, token).header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::json!({"chunks": [
            {"content": "a".repeat(200_000)},
            {"content": "b".repeat(200_000)}
        ]}).to_string())).unwrap();
    assert_eq!(app.clone().oneshot(replace).await.unwrap().status(), StatusCode::OK);
    let page = Request::builder().uri(format!("/api/notes/{note_id}/chunks?limit=8&lightweight=true"))
        .header(header::AUTHORIZATION, token).body(Body::empty()).unwrap();
    let response = app.clone().oneshot(page).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let page: Value = serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(page["chunks"].as_array().unwrap().len(), 1);
    assert_eq!(page["has_more"], true);
    let chunk_id = page["chunks"][0]["id"].as_u64().unwrap();
    let large_content = "x".repeat(2 * 1024 * 1024 + 1024);
    let edit = Request::builder().method("PUT").uri(format!("/api/notes/{note_id}/chunks/{chunk_id}"))
        .header(header::AUTHORIZATION, token).header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::json!({"content": large_content}).to_string())).unwrap();
    let response = app.clone().oneshot(edit).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let updated: Value = serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(updated["data"]["chunk"]["content"].as_str().unwrap().len(), large_content.len());
    for _ in 0..2 {
        let get = Request::builder().uri(format!("/api/notes/{note_id}"))
            .header(header::AUTHORIZATION, token).body(Body::empty()).unwrap();
        let response = app.clone().oneshot(get).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let note: Value = serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
        assert_eq!(note["data"]["chunks"][0]["content"], large_content);
        assert_eq!(note["data"]["content"].as_str().unwrap().len(), large_content.len() + 200_000);
    }
    let delete = Request::builder().method("DELETE").uri(format!("/api/notes/{note_id}"))
        .header(header::AUTHORIZATION, token).body(Body::empty()).unwrap();
    assert_eq!(app.oneshot(delete).await.unwrap().status(), StatusCode::OK);
}
