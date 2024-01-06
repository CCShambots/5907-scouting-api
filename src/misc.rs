use crate::datatypes::ItemPath;
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
    ItemPath(path): ItemPath,
    Query(format): Query<AgeQuery>,
    storage_manager: Extension<Arc<StorageManager>>,
) -> impl IntoResponse {
    match path {
        None => StatusCode::BAD_REQUEST.into_response(),
        Some(path) => match metadata(format!("{}/{}", storage_manager.get_path(), path)).await {
            Ok(metadata) => {
                let file_timestamp = metadata
                    .created()
                    .unwrap()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64;
                let now_timestamp = Utc::now().timestamp();

                match format.format {
                    Format::Days => (
                        StatusCode::OK,
                        Json(Duration::seconds(now_timestamp - file_timestamp).num_days()),
                    )
                        .into_response(),
                    Format::Timestamp => (StatusCode::OK, Json(file_timestamp)).into_response(),
                }
            }
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        },
    }
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
