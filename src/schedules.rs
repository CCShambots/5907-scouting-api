use crate::datatypes::Schedule;
use crate::storage_manager::StorageManager;
use crate::transactions::DataType;
use anyhow::Error;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use std::sync::Arc;
use tracing::instrument;

#[instrument(skip(schedule, storage_manager))]
pub async fn add_schedule(
    storage_manager: Extension<Arc<StorageManager>>,
    Json(schedule): Json<Schedule>,
) -> SchedulesResponse {
    match storage_manager.storable_add(schedule).await {
        Ok(_) => SchedulesResponse::OK,
        Err(_) => SchedulesResponse::FailedToAdd,
    }
}

#[instrument(skip(storage_manager))]
pub async fn get_schedule(
    Path(name): Path<String>,
    storage_manager: Extension<Arc<StorageManager>>,
) -> SchedulesResponse {
    match storage_manager
        .storable_get_serialized(name, DataType::Schedule)
        .await
    {
        Ok(t) => SchedulesResponse::SerializedSchedule(t),
        Err(_) => SchedulesResponse::FailedToRead,
    }
}

#[instrument(skip(storage_manager, schedule))]
pub async fn edit_schedule(
    storage_manager: Extension<Arc<StorageManager>>,
    Json(schedule): Json<Schedule>,
) -> SchedulesResponse {
    match storage_manager.storable_edit(schedule).await {
        Ok(_) => SchedulesResponse::OK,
        Err(_) => SchedulesResponse::FailedToEdit,
    }
}

#[instrument(skip(storage_manager))]
pub async fn list_schedules(storage_manager: Extension<Arc<StorageManager>>) -> SchedulesResponse {
    match storage_manager.storable_list(DataType::Schedule).await {
        Ok(l) => SchedulesResponse::List(l),
        Err(_) => SchedulesResponse::FailedToRead,
    }
}

#[instrument(skip(storage_manager))]
pub async fn delete_schedule(
    Path(name): Path<String>,
    storage_manager: Extension<Arc<StorageManager>>,
) -> SchedulesResponse {
    match storage_manager
        .storable_delete(&name, DataType::Schedule)
        .await
    {
        Ok(_) => SchedulesResponse::OK,
        Err(_) => SchedulesResponse::FailedToDelete,
    }
}

#[derive(Debug)]
pub enum SchedulesResponse {
    OK,
    SerializedSchedule(Vec<u8>),
    List(Vec<String>),
    FailedToAdd,
    FailedToEdit,
    FailedToDelete,
    FailedToRead,
}

impl IntoResponse for SchedulesResponse {
    fn into_response(self) -> Response {
        match self {
            SchedulesResponse::OK => StatusCode::OK.into_response(),
            SchedulesResponse::SerializedSchedule(t) => {
                (StatusCode::OK, String::from_utf8(t).unwrap()).into_response()
            }
            SchedulesResponse::FailedToAdd => StatusCode::BAD_REQUEST.into_response(),
            SchedulesResponse::FailedToEdit => StatusCode::BAD_REQUEST.into_response(),
            SchedulesResponse::FailedToDelete => StatusCode::BAD_REQUEST.into_response(),
            SchedulesResponse::FailedToRead => StatusCode::BAD_REQUEST.into_response(),
            SchedulesResponse::List(l) => (StatusCode::OK, Json(l)).into_response(),
        }
    }
}
