use crate::datatypes::Schedule;
use crate::storage_manager::StorageManager;
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
    match storage_manager.schedules_add(schedule).await {
        Ok(_) => SchedulesResponse::OK,
        Err(_) => SchedulesResponse::FailedToAdd,
    }
}

#[instrument(skip(storage_manager))]
pub async fn get_schedule(
    Path(name): Path<String>,
    storage_manager: Extension<Arc<StorageManager>>,
) -> SchedulesResponse {
    match storage_manager.schedules_get(name).await {
        Ok(t) => SchedulesResponse::Schedule(t),
        Err(_) => SchedulesResponse::FailedToRead,
    }
}

#[instrument(skip(storage_manager, schedule))]
pub async fn edit_schedule(
    storage_manager: Extension<Arc<StorageManager>>,
    Json(schedule): Json<Schedule>,
) -> SchedulesResponse {
    match storage_manager.schedules_edit(schedule).await {
        Ok(_) => SchedulesResponse::OK,
        Err(_) => SchedulesResponse::FailedToEdit,
    }
}

#[instrument(skip(storage_manager))]
pub async fn list_schedules(storage_manager: Extension<Arc<StorageManager>>) -> SchedulesResponse {
    match storage_manager.schedules_list().await {
        Ok(l) => SchedulesResponse::List(l),
        Err(_) => SchedulesResponse::FailedToRead,
    }
}

#[instrument(skip(storage_manager))]
pub async fn delete_schedule(
    Path(name): Path<String>,
    storage_manager: Extension<Arc<StorageManager>>,
) -> SchedulesResponse {
    match storage_manager.schedules_delete(name).await {
        Ok(_) => SchedulesResponse::OK,
        Err(_) => SchedulesResponse::FailedToDelete,
    }
}

#[derive(Debug)]
pub enum SchedulesResponse {
    OK,
    Schedule(Schedule),
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
            SchedulesResponse::Schedule(t) => (StatusCode::OK, Json(t)).into_response(),
            SchedulesResponse::FailedToAdd => StatusCode::BAD_REQUEST.into_response(),
            SchedulesResponse::FailedToEdit => StatusCode::BAD_REQUEST.into_response(),
            SchedulesResponse::FailedToDelete => StatusCode::BAD_REQUEST.into_response(),
            SchedulesResponse::FailedToRead => StatusCode::BAD_REQUEST.into_response(),
            SchedulesResponse::List(l) => (StatusCode::OK, Json(l)).into_response(),
        }
    }
}
