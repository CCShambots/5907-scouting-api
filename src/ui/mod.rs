mod state;

use std::sync::Arc;
use askama::Template;
use axum::{Extension, Form};
use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use crate::storage_manager::StorageManager;
use crate::transactions::{Action, DataType, Transaction};
use crate::ui::state::{EntryType, InterfaceManager};

const MAIN_PAGE: &str = include_str!("../../static/htmx/index.html");

#[derive(Template, Clone)]
#[template(path = "alt-key-search.html")]
struct SearchTemplate {
    transactions: Vec<TransactionTemplate>,
}

#[derive(Clone)]
struct TransactionTemplate {
    alt_key: String,
    timestamp: i64,
    action: String,
    badge_bs_style: String,
    cache_id: String,
    data_type: String,
    type_badge_style: String,
}

#[derive(Serialize, Deserialize)]
struct Search {
    search: String,
}

impl From<Transaction> for TransactionTemplate {
    fn from(value: Transaction) -> Self {
        Self {
            alt_key: value.alt_key,
            timestamp: value.timestamp,
            action: value.action.to_string(),
            badge_bs_style: match value.action {
                Action::Add => "text-bg-success".into(),
                Action::Delete => "text-bg-danger".into(),
                Action::Edit => "text-bg-warning".into()
            },
            cache_id: Uuid::new_v4().to_string(),
            data_type: format!("{:?}", value.data_type),
            type_badge_style: match value.data_type {
                DataType::Bytes => "text-bg-danger".into(),
                DataType::Form => "text-bg-success".into(),
                DataType::Schedule => "text-bg-warning".into(),
                DataType::Template => "text-bg-primary".into()
            },
        }
    }
}

pub async fn search(Path(cache_id): Path<String>, Form(search): Form<Search>, interface_manager: Extension<Arc<InterfaceManager>>, storage_manager: Extension<Arc<StorageManager>>) -> impl IntoResponse {
    if let Some(cache) = interface_manager.check_cache(&cache_id).await {
        let count = storage_manager.search_count(&search.search).await.unwrap();
        if cache.count != count || cache.entry_type != EntryType::Search(search.search.clone()) {
            interface_manager.clear_cache_entry(&cache_id).await;
            interface_manager.set_cache(cache_id.clone(), EntryType::Search(search.search.clone()), count).await;
        } else {
            return StatusCode::NO_CONTENT.into_response();
        }
    }

    let search_res: Vec<TransactionTemplate> = storage_manager.search(&search.search)
        .await
        .unwrap()
        .into_iter()
        .map(|t| TransactionTemplate::from(t))
        .collect();

    let template = SearchTemplate {
        transactions: search_res
    };

    let html = template.render().unwrap().clone();


    Html(html).into_response()
}

pub async fn ui_main() -> impl IntoResponse {
    Html(MAIN_PAGE)
}