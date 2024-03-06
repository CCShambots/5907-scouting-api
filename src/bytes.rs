use crate::datatypes::BytesReference;
use crate::storage_manager::StorageManager;
use crate::transactions::DataType;
use anyhow::Error;
use axum::body::Bytes;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Extension;
use std::sync::Arc;
use tracing::{info, instrument};

#[instrument(skip(storage_manager, parts))]
pub async fn store_bytes(
    Path(blob_id): Path<String>,
    storage_manager: Extension<Arc<StorageManager>>,
    parts: Bytes,
) -> StoreBytesResponse {
    match storage_manager
        .storable_add(BytesReference(blob_id, parts.to_vec()))
        .await
    {
        Ok(_) => StoreBytesResponse::OK,
        Err(_) => StoreBytesResponse::FailedToWriteBlob,
    }
}

#[instrument(skip(storage_manager))]
pub async fn get_bytes(
    Path(blob_id): Path<String>,
    storage_manager: Extension<Arc<StorageManager>>,
) -> StoreBytesResponse {
    match storage_manager
        .storable_get_serialized(blob_id, DataType::Bytes)
        .await
    {
        Ok(bytes) => StoreBytesResponse::Data(bytes),
        Err(_) => StoreBytesResponse::NotFound,
    }
}

#[instrument(skip(storage_manager))]
pub async fn delete_bytes(
    Path(blob_id): Path<String>,
    storage_manager: Extension<Arc<StorageManager>>,
) -> StoreBytesResponse {
    let blob_id = blob_id.clone();

    let blob_id = sha256::digest(blob_id);

    let _ = storage_manager
        .storable_delete(&blob_id, DataType::Bytes)
        .await;

    StoreBytesResponse::DeleteSuccess
}

#[instrument(skip(storage_manager, parts))]
pub async fn edit_bytes(
    Path(blob_id): Path<String>,
    storage_manager: Extension<Arc<StorageManager>>,
    parts: Bytes,
) -> StoreBytesResponse {
    match storage_manager
        .storable_edit(BytesReference(blob_id, parts.to_vec()))
        .await
    {
        Ok(_) => StoreBytesResponse::OK,
        Err(_) => StoreBytesResponse::FailedToEdit,
    }
}

#[instrument(skip(storage_manager))]
pub async fn list_bytes(storage_manager: Extension<Arc<StorageManager>>) -> StoreBytesResponse {
    match storage_manager.storable_list(DataType::Bytes).await {
        Ok(list) => StoreBytesResponse::List(serde_json::to_string(&list).unwrap()),
        Err(_) => StoreBytesResponse::FailedToReadBlobs,
    }
}

#[derive(Debug)]
pub enum StoreBytesResponse {
    OK,
    FailedToWriteBlob,
    Data(Vec<u8>),
    List(String),
    NotFound,
    DeleteSuccess,
    FailedToEdit,
    FailedToReadBlobs,
}

impl IntoResponse for StoreBytesResponse {
    fn into_response(self) -> Response {
        match self {
            StoreBytesResponse::OK => StatusCode::OK.into_response(),
            StoreBytesResponse::FailedToWriteBlob => {
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
            StoreBytesResponse::Data(data) => (StatusCode::OK, data).into_response(),
            StoreBytesResponse::NotFound => StatusCode::BAD_REQUEST.into_response(),
            StoreBytesResponse::DeleteSuccess => StatusCode::OK.into_response(),
            StoreBytesResponse::FailedToEdit => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            StoreBytesResponse::List(list) => (StatusCode::OK, list).into_response(),
            StoreBytesResponse::FailedToReadBlobs => {
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        }
    }
}
