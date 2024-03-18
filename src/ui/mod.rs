mod state;

use std::sync::Arc;
use askama::Template;
use axum::{Extension, Form};
use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use crate::storage_manager::StorageManager;
use crate::transactions::{Action, DataType, Transaction};
use crate::ui::state::{EntryType, InterfaceCacheEntry, InterfaceManager};

const MAIN_PAGE: &str = include_str!("../../static/htmx/index.html");
const SEARCH_PAGE: &str = include_str!("../../static/htmx/search-page.html");

#[derive(Template, Clone)]
#[template(path = "alt-key-search.html")]
struct SearchTemplate {
    transactions: Vec<AltKeyRow>,
}

#[derive(Template)]
#[template(path = "alt-key-row.html")]
struct AltKeyRowTemplate {
    transaction: AltKeyRow,
}

#[derive(Clone)]
struct AltKeyRow {
    alt_key: String,
    timestamp: i64,
    action: String,
    action_badge_style: String,
    cache_id: String,
    data_type: String,
    type_badge_style: String,
}

#[derive(Serialize, Deserialize)]
struct Search {
    search: String,
}

impl From<Transaction> for AltKeyRow {
    fn from(value: Transaction) -> Self {
        Self {
            alt_key: value.alt_key,
            timestamp: value.timestamp,
            action: value.action.to_string(),
            action_badge_style: match value.action {
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
    let cache = interface_manager.get_cache();

    if let Some(cached) = cache.get(&cache_id).await {
        let count = storage_manager.search_count(&search.search).await.unwrap();
        if cached.count != count || cached.entry_type != EntryType::Search(search.search.clone()) {
            cache.insert(cache_id, InterfaceCacheEntry {
                timestamp: Utc::now(),
                entry_type: EntryType::Search(search.search.clone()),
                count,
            }).await;
        } else {
            return StatusCode::NO_CONTENT.into_response();
        }
    }

    let search_res: Vec<AltKeyRow> = storage_manager.search(&search.search)
        .await
        .unwrap()
        .into_iter()
        .map(|t| AltKeyRow::from(t))
        .collect();

    let template = SearchTemplate {
        transactions: search_res
    };

    let html = template.render().unwrap();


    Html(html).into_response()
}

pub async fn get_alt_key_row(Path((alt_key, data_type, cache_id)): Path<(String, DataType, String)>, interface_manager: Extension<InterfaceManager>, storage_manager: Extension<StorageManager>) -> impl IntoResponse {
    let cache = interface_manager.get_cache();

    if let Some(cached) = cache.get(&cache_id).await {
        let count = storage_manager.transaction_count(&alt_key, data_type).await.unwrap();
        if cached.count != count || cached.entry_type != EntryType::AltKeyTableEntry(alt_key.clone(), data_type) {
            cache.insert(cache_id, InterfaceCacheEntry {
                timestamp: Utc::now(),
                entry_type: EntryType::AltKeyTableEntry(alt_key.clone(), data_type),
                count,
            }).await;
        } else {
            return StatusCode::NO_CONTENT.into_response();
        }
    }

    let row: AltKeyRow = storage_manager.latest_transaction_from_alt_key(&alt_key, data_type)
        .await
        .unwrap()
        .into();

    let template = AltKeyRowTemplate {
        transaction: row
    };

    let html = template.render().unwrap();

    Html(html).into_response()
}

pub async fn search_page() -> impl IntoResponse {
    Html(SEARCH_PAGE)
}

pub async fn ui_main() -> impl IntoResponse {
    Html(MAIN_PAGE)
}