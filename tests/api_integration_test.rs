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
async fn test_crud_and_progressive_chunk_loading_with_sanctum_token() {
    let config = Config::from_env();
    let app = create_app(&config).await.expect("Failed to create app");

    let token = "1|sanctum_test_token_1234567890abcdef";

    // 1. Create a note with large chunkable content
    let content = r#"<p>Bab 1: Permulaan</p>
<p>Ini adalah naskah kuno yang sangat panjang dan mempesona.</p>
<blockquote>Petualangan dimulai ketika fajar menyingsing.</blockquote>
<pre><code>let journey = true;</code></pre>
"#;

    let payload = serde_json::json!({
        "title": "Scroll of Eternity",
        "category": "catatan",
        "content": content,
        "font_family": "sans",
        "font_size": "16px",
        "theme_color": "blue",
    });

    let create_req = Request::builder()
        .method("POST")
        .uri("/api/notes")
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(payload.to_string()))
        .unwrap();

    let create_resp = app.clone().oneshot(create_req).await.unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);

    let bytes = create_resp.into_body().collect().await.unwrap().to_bytes();
    let created_json: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(created_json["status"], "success");
    let note_id = created_json["data"]["id"].as_u64().unwrap();
    assert!(note_id > 0);
    assert_eq!(created_json["data"]["title"], "Scroll of Eternity");
    assert!(created_json["data"]["chunks"].as_array().unwrap().len() >= 1);

    // 2. Progressive chunk loading: fetch chunks with limit=1, offset=0
    let chunk_slice_req = Request::builder()
        .uri(format!("/api/notes/{}/chunks?offset=0&limit=1", note_id))
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();

    let chunk_slice_resp = app.clone().oneshot(chunk_slice_req).await.unwrap();
    assert_eq!(chunk_slice_resp.status(), StatusCode::OK);
    let bytes = chunk_slice_resp.into_body().collect().await.unwrap().to_bytes();
    let chunk_slice_json: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(chunk_slice_json["status"], "success");
    assert_eq!(chunk_slice_json["limit"], 1);
    assert_eq!(chunk_slice_json["offset"], 0);
    assert_eq!(chunk_slice_json["chunks"].as_array().unwrap().len(), 1);

    // 3. Verify chunks cryptographic integrity
    let verify_req = Request::builder()
        .uri(format!("/api/notes/{}/chunks/verify", note_id))
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();

    let verify_resp = app.clone().oneshot(verify_req).await.unwrap();
    assert_eq!(verify_resp.status(), StatusCode::OK);
    let bytes = verify_resp.into_body().collect().await.unwrap().to_bytes();
    let verify_json: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(verify_json["data"]["status"], "verified");
    assert_eq!(verify_json["data"]["verified"], true);

    // 4. Search notes by chunk content directly in MySQL
    let search_req = Request::builder()
        .uri("/api/notes?q=mempesona")
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();

    let search_resp = app.clone().oneshot(search_req).await.unwrap();
    assert_eq!(search_resp.status(), StatusCode::OK);
    let bytes = search_resp.into_body().collect().await.unwrap().to_bytes();
    let search_json: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(search_json["status"], "success");
    let search_items = search_json["data"].as_array().unwrap();
    assert!(search_items.iter().any(|item| item["id"] == note_id));

    // 5. Duplicate note with chunks
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
    assert_eq!(dup_json["status"], "success");
    let cloned_id = dup_json["data"]["id"].as_u64().unwrap();
    assert_ne!(cloned_id, note_id);
    assert_eq!(dup_json["data"]["title"], "Scroll of Eternity (Copy)");
    let cloned_chunks = dup_json["data"]["chunks"].as_array().unwrap();
    assert_eq!(
        cloned_chunks.len(),
        created_json["data"]["chunks"].as_array().unwrap().len()
    );

    // 6. Clean up: Delete cloned note and original note
    let delete_cloned_req = Request::builder()
        .method("DELETE")
        .uri(format!("/api/notes/{}", cloned_id))
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();
    let delete_cloned_resp = app.clone().oneshot(delete_cloned_req).await.unwrap();
    assert_eq!(delete_cloned_resp.status(), StatusCode::OK);

    let delete_req = Request::builder()
        .method("DELETE")
        .uri(format!("/api/notes/{}", note_id))
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();

    let delete_resp = app.clone().oneshot(delete_req).await.unwrap();
    assert_eq!(delete_resp.status(), StatusCode::OK);
}
