use crate::storage_manager::StorageManager;
use crate::transactions::InternalMessage;
use anyhow::Error;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tracing::{info, instrument};
use uuid::Uuid;

#[instrument(skip(storage_manager))]
pub async fn sync(
    last_id: Option<Path<Uuid>>,
    storage_manager: Extension<Arc<StorageManager>>,
) -> SyncResponse {
    info!("hello :)");

    match last_id {
        None => match storage_manager.get_first().await {
            Ok(msg) => SyncResponse::OK(msg),
            Err(_) => SyncResponse::NotFound,
        },
        Some(id) => match storage_manager.get_after(id.0).await {
            Ok(msg) => SyncResponse::OK(msg),
            Err(_) => SyncResponse::NotFound,
        },
    }
}

#[instrument(skip(storage_manager))]
pub async fn get_file(
    Path(path): Path<String>,
    storage_manager: Extension<Arc<StorageManager>>,
) -> SyncResponse {
    match storage_manager.get_file(path).await {
        Ok(f) => SyncResponse::File(f),
        Err(_) => SyncResponse::NotFound,
    }
}

#[instrument(skip(storage_manager))]
pub async fn list_files(storage_manager: Extension<Arc<StorageManager>>) -> SyncResponse {
    match storage_manager.list_files().await {
        Ok(files) => SyncResponse::Files(files),
        Err(_) => SyncResponse::Internal,
    }
}

impl IntoResponse for SyncResponse {
    fn into_response(self) -> Response {
        match self {
            SyncResponse::OK(msg) => Json(msg).into_response(),
            SyncResponse::NotFound => StatusCode::NOT_FOUND.into_response(),
            SyncResponse::File(f) => (StatusCode::OK, f).into_response(),
            SyncResponse::Files(f) => Json(f).into_response(),
            SyncResponse::Internal => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    }
}

pub enum SyncResponse {
    OK(InternalMessage),
    File(Vec<u8>),
    Files(Vec<String>),
    NotFound,
    Internal,
}
