use std::collections::{HashMap, HashSet};
use crate::storage_manager::StorageManager;
use crate::transactions::InternalMessage;
use anyhow::Error;
use axum::extract::{FromRequestParts, Path};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{async_trait, Extension, Json};
use std::sync::Arc;
use axum::http::request::Parts;
use serde::Deserialize;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tracing::{info, instrument};
use uuid::Uuid;

#[instrument(skip(storage_manager))]
pub async fn diff(
    storage_manager: Extension<Arc<StorageManager>>,
    sync_manager: Extension<Arc<SyncManager>>,
    child_id: ChildID,
    Json(glob): Json<Vec<String>>,
) -> SyncResponse {
    if !sync_manager.valid_child_id(&child_id) { return SyncResponse::UnapprovedChild; }

    let self_glob = storage_manager.glob().await.unwrap();

    if glob.is_empty() { return SyncResponse::Diff(self_glob, vec![]); }

    let have: Vec<String> = self_glob.iter().cloned().filter(|s| !glob.contains(s)).collect();
    let need: Vec<String> = glob.into_iter().filter(|s| !self_glob.contains(s)).collect();

    SyncResponse::Diff(have, need)
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

impl IntoResponse for SyncResponse {
    fn into_response(self) -> Response {
        match self {
            SyncResponse::OK(msg) => Json(msg).into_response(),
            SyncResponse::NotFound => StatusCode::NOT_FOUND.into_response(),
            SyncResponse::File(f) => (StatusCode::OK, f).into_response(),
            SyncResponse::Diff(have, need) => Json((have, need)).into_response(),
            SyncResponse::Internal => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            SyncResponse::UnapprovedChild => StatusCode::UNAUTHORIZED.into_response()
        }
    }
}

pub enum SyncResponse {
    OK(InternalMessage),
    File(Vec<u8>),
    Diff(Vec<String>, Vec<String>),
    UnapprovedChild,
    NotFound,
    Internal,
}

#[derive(Debug, Deserialize, PartialEq, Eq, Hash)]
pub struct ChildID(Uuid);

#[derive(Deserialize, Debug)]
pub struct SyncManager {
    approved_children: HashSet<ChildID>,
    parents: Vec<String>,
    id: ChildID,
}

impl SyncManager {
    fn valid_child_id(&self, id: &ChildID) -> bool {
        self.approved_children.contains(id)
    }
}

#[async_trait]
impl<S> FromRequestParts<S> for ChildID {
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        match parts.headers.get("child_id") {
            None => Err((StatusCode::UNAUTHORIZED, "missing child_id").into_response()),
            Some(value) => match Uuid::parse_str(value.to_str().unwrap()) {
                Ok(id) => Ok(ChildID(id)),
                Err(_) => Err((StatusCode::BAD_REQUEST, "child_id incorrectly formatted").into_response())
            }
        }
    }
}
