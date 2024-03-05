use crate::datatypes::FormTemplate;
use crate::storage_manager::StorageManager;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use std::sync::Arc;
use tracing::instrument;
use crate::transactions::DataType;

#[instrument(skip(template, storage_manager))]
pub async fn add_template(
    storage_manager: Extension<Arc<StorageManager>>,
    Json(template): Json<FormTemplate>,
) -> TemplatesResponse {
    match storage_manager.storable_add(template).await {
        Ok(_) => TemplatesResponse::OK,
        Err(_) => TemplatesResponse::FailedToAdd,
    }
}

#[instrument(skip(storage_manager))]
pub async fn get_template(
    Path(name): Path<String>,
    storage_manager: Extension<Arc<StorageManager>>,
) -> TemplatesResponse {
    match storage_manager.storable_get_serialized(name, DataType::Template).await {
        Ok(t) => TemplatesResponse::SerializedTemplate(t),
        Err(_) => TemplatesResponse::FailedToRead,
    }
}

#[instrument(skip(storage_manager, template))]
pub async fn edit_template(
    storage_manager: Extension<Arc<StorageManager>>,
    Json(template): Json<FormTemplate>,
) -> TemplatesResponse {
    match storage_manager.storable_edit(template).await {
        Ok(_) => TemplatesResponse::OK,
        Err(_) => TemplatesResponse::FailedToEdit,
    }
}

#[instrument(skip(storage_manager))]
pub async fn list_templates(storage_manager: Extension<Arc<StorageManager>>) -> TemplatesResponse {
    match storage_manager.storable_list(DataType::Template).await {
        Ok(l) => TemplatesResponse::List(l),
        Err(_) => TemplatesResponse::FailedToRead,
    }
}

#[instrument(skip(storage_manager))]
pub async fn delete_template(
    Path(name): Path<String>,
    storage_manager: Extension<Arc<StorageManager>>,
) -> TemplatesResponse {
    match storage_manager.storable_delete(&name, DataType::Template).await {
        Ok(_) => TemplatesResponse::OK,
        Err(_) => TemplatesResponse::FailedToDelete,
    }
}

#[derive(Debug)]
pub enum TemplatesResponse {
    OK,
    SerializedTemplate(Vec<u8>),
    List(Vec<String>),
    FailedToAdd,
    FailedToEdit,
    FailedToDelete,
    FailedToRead,
}

impl IntoResponse for TemplatesResponse {
    fn into_response(self) -> Response {
        match self {
            TemplatesResponse::OK => StatusCode::OK.into_response(),
            TemplatesResponse::SerializedTemplate(t) => (StatusCode::OK, String::from_utf8(t).unwrap()).into_response(),
            TemplatesResponse::FailedToAdd => StatusCode::BAD_REQUEST.into_response(),
            TemplatesResponse::FailedToEdit => StatusCode::BAD_REQUEST.into_response(),
            TemplatesResponse::FailedToDelete => StatusCode::BAD_REQUEST.into_response(),
            TemplatesResponse::FailedToRead => StatusCode::BAD_REQUEST.into_response(),
            TemplatesResponse::List(l) => (StatusCode::OK, Json(l)).into_response(),
        }
    }
}
