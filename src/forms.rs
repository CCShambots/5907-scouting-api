use crate::datatypes::{Filter, Form, Schedule};
use crate::storage_manager::StorageManager;
use anyhow::Error;
use axum::extract::{Path, Query};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use datafusion::arrow::compute::filter;
use std::sync::Arc;
use tracing::{info, instrument};
use uuid::Uuid;

#[instrument(skip(form, storage_manager))]
pub async fn add_form(
    Path(template): Path<String>,
    storage_manager: Extension<Arc<StorageManager>>,
    Json(form): Json<Form>,
) -> FormsResponse {
    match storage_manager.forms_add(template, form).await {
        Ok(id) => FormsResponse::ID(id),
        Err(_) => FormsResponse::FailedToAdd,
    }
}

#[instrument(skip(storage_manager))]
pub async fn list_forms(
    Path(template): Path<String>,
    storage_manager: Extension<Arc<StorageManager>>,
) -> FormsResponse {
    match storage_manager.forms_list(template).await {
        Ok(l) => FormsResponse::IDList(l),
        Err(_) => FormsResponse::FailedToRead
    }
}

#[instrument(skip(storage_manager))]
pub async fn get_form(
    Path((template, name)): Path<(String, String)>,
    storage_manager: Extension<Arc<StorageManager>>,
) -> FormsResponse {
    match storage_manager.forms_get_serialized(template, name).await {
        Ok(t) => FormsResponse::SerializedForm(t),
        Err(_) => FormsResponse::FailedToRead,
    }
}

#[instrument(skip(storage_manager, form))]
pub async fn edit_form(
    Path((template, id)): Path<(String, String)>,
    storage_manager: Extension<Arc<StorageManager>>,
    Json(form): Json<Form>,
) -> FormsResponse {
    match storage_manager.forms_edit(template, form, id).await {
        Ok(_) => FormsResponse::OK,
        Err(_) => FormsResponse::FailedToEdit,
    }
}

#[instrument(skip(storage_manager))]
pub async fn filter_forms(
    Path(template): Path<String>,
    Query(filter): Query<Filter>,
    storage_manager: Extension<Arc<StorageManager>>,
) -> FormsResponse {
    info!("Filter: {:?}", filter);

    match storage_manager.forms_filter(template, filter).await {
        Ok(l) => FormsResponse::Filtered(l),
        Err(_) => FormsResponse::FailedToRead,
    }
}

#[instrument(skip(storage_manager))]
pub async fn delete_form(
    Path((template, name)): Path<(String, String)>,
    storage_manager: Extension<Arc<StorageManager>>,
) -> FormsResponse {
    match storage_manager.forms_delete(template, name).await {
        Ok(_) => FormsResponse::OK,
        Err(_) => FormsResponse::FailedToDelete,
    }
}

#[derive(Debug)]
pub enum FormsResponse {
    OK,
    ID(Uuid),
    IDList(Vec<Uuid>),
    SerializedForm(Vec<u8>),
    Filtered(Vec<Form>),
    FailedToAdd,
    FailedToEdit,
    FailedToDelete,
    FailedToRead,
}

impl IntoResponse for FormsResponse {
    fn into_response(self) -> Response {
        match self {
            FormsResponse::OK => StatusCode::OK.into_response(),
            FormsResponse::FailedToAdd => StatusCode::BAD_REQUEST.into_response(),
            FormsResponse::FailedToEdit => StatusCode::BAD_REQUEST.into_response(),
            FormsResponse::FailedToDelete => StatusCode::BAD_REQUEST.into_response(),
            FormsResponse::FailedToRead => StatusCode::BAD_REQUEST.into_response(),
            FormsResponse::Filtered(l) => (StatusCode::OK, Json(l)).into_response(),
            FormsResponse::ID(id) => (StatusCode::OK, Json(id)).into_response(),
            FormsResponse::IDList(ids) => (StatusCode::OK, Json(ids)).into_response(),
            FormsResponse::SerializedForm(ser) => (StatusCode::OK, String::from_utf8(ser).unwrap()).into_response()
        }
    }
}
