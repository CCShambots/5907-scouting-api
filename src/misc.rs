use crate::storage_manager::StorageManager;
use axum::extract::Query;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{Extension, Json};
use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::UNIX_EPOCH;
use tokio::fs;
use tokio::fs::metadata;
use tokio::time::Instant;
use tracing::instrument;

#[instrument(ret, skip(storage_manager))]
pub async fn age(
    Query(format): Query<AgeQuery>,
    storage_manager: Extension<Arc<StorageManager>>,
) -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AgeQuery {
    #[serde(alias = "type")]
    format: Format,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Format {
    #[serde(alias = "days")]
    Days,
    #[serde(alias = "timestamp")]
    Timestamp,
}
