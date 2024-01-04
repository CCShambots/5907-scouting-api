use crate::datatypes::FormTemplate;
use crate::storage_manager::StorageManager;
use anyhow::Error;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use std::sync::Arc;
use tracing::instrument;

#[instrument(skip(template, storage_manager))]
pub async fn add_template(
    storage_manager: Extension<Arc<StorageManager>>,
    Json(template): Json<FormTemplate>,
) -> TemplatesResponse {
    match storage_manager.templates_add(template).await {
        Ok(_) => TemplatesResponse::OK,
        Err(_) => TemplatesResponse::FailedToAdd,
    }
}

#[instrument(skip(storage_manager))]
pub async fn get_template(
    Path(name): Path<String>,
    storage_manager: Extension<Arc<StorageManager>>,
) -> TemplatesResponse {
    match storage_manager.templates_get(name).await {
        Ok(t) => TemplatesResponse::Template(t),
        Err(_) => TemplatesResponse::FailedToRead,
    }
}

#[instrument(skip(storage_manager, template))]
pub async fn edit_template(
    storage_manager: Extension<Arc<StorageManager>>,
    Json(template): Json<FormTemplate>,
) -> TemplatesResponse {
    match storage_manager.templates_edit(template).await {
        Ok(_) => TemplatesResponse::OK,
        Err(_) => TemplatesResponse::FailedToEdit,
    }
}

#[instrument(skip(storage_manager))]
pub async fn list_templates(storage_manager: Extension<Arc<StorageManager>>) -> TemplatesResponse {
    match storage_manager.templates_list().await {
        Ok(l) => TemplatesResponse::List(l),
        Err(_) => TemplatesResponse::FailedToRead,
    }
}

#[instrument(skip(storage_manager))]
pub async fn delete_template(
    Path(name): Path<String>,
    storage_manager: Extension<Arc<StorageManager>>,
) -> TemplatesResponse {
    match storage_manager.templates_delete(name).await {
        Ok(_) => TemplatesResponse::OK,
        Err(_) => TemplatesResponse::FailedToDelete,
    }
}

#[derive(Debug)]
pub enum TemplatesResponse {
    OK,
    Template(FormTemplate),
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
            TemplatesResponse::Template(t) => (StatusCode::OK, Json(t)).into_response(),
            TemplatesResponse::FailedToAdd => StatusCode::BAD_REQUEST.into_response(),
            TemplatesResponse::FailedToEdit => StatusCode::BAD_REQUEST.into_response(),
            TemplatesResponse::FailedToDelete => StatusCode::BAD_REQUEST.into_response(),
            TemplatesResponse::FailedToRead => StatusCode::BAD_REQUEST.into_response(),
            TemplatesResponse::List(l) => (StatusCode::OK, Json(l)).into_response(),
        }
    }
}
