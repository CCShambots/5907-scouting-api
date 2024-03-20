use crate::storage_manager::StorageManager;
use anyhow::Error;
use axum::extract::{FromRequestParts, Path};
use axum::http::request::Parts;
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{async_trait, Extension, Json};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use reqwest::{Client, RequestBuilder};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tracing::{info, instrument};
use uuid::Uuid;

#[instrument(skip(sync_manager))]
pub async fn diff(
    sync_manager: Extension<Arc<SyncManager>>,
    child_id: ChildID,
    Json(glob): Json<Vec<String>>,
) -> SyncResponse {
    SyncResponse::Error
}

impl IntoResponse for SyncResponse {
    fn into_response(self) -> Response {
        match self {
            SyncResponse::Error => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            SyncResponse::Diff(have, need) => Json((have, need)).into_response(),
        }
    }
}

pub enum SyncResponse {
    Diff(Vec<String>, Vec<String>),
    Error,
}

#[derive(Debug, Deserialize, PartialEq, Eq, Hash)]
pub struct ChildID(Uuid);

pub struct SyncManager {
    pub approved_children: HashSet<ChildID>,
    pub parent: Option<String>,
    pub id: ChildID,
    pub storage_manager: Arc<StorageManager>,
}

impl SyncManager {
    fn valid_child_id(&self, id: &ChildID) -> bool {
        self.approved_children.contains(id)
    }

    async fn start_sync(self) -> Result<(), anyhow::Error> {
        if self.parent.is_none() {
            return Ok(());
        }

        let parent = self.parent.unwrap();
        let parent_id: ChildID = reqwest::get(format!("{}/sync/id", parent))
            .await?
            .json()
            .await?;

        let client = Client::new();
        let last_txid: Option<Uuid> = todo!();

        loop {
            let req = match last_txid {
                None => client
                    .get(format!("{}/sync/last_since", parent))
                    .header("child_id", self.id.0.to_string())
                    .build()?,
                Some(txid) => client
                    .get(format!("{}/sync/last_since", parent))
                    .header("child_id", self.id.0.to_string())
                    .header("txid", txid.to_string())
                    .build()?,
            };
        }
    }
}

#[async_trait]
impl<S> FromRequestParts<S> for ChildID {
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        match parts.headers.get("child_id") {
            None => Err((StatusCode::UNAUTHORIZED, "missing child_id").into_response()),
            Some(value) => match Uuid::parse_str(value.to_str().unwrap()) {
                Ok(id) => Ok(ChildID(id)),
                Err(_) => {
                    Err((StatusCode::BAD_REQUEST, "child_id incorrectly formatted").into_response())
                }
            },
        }
    }
}
