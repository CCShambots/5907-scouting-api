use askama::Template;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse};
use chrono::Utc;

const MAIN_PAGE: &str = include_str!("../../static/htmx/index.html");

#[derive(Template, Clone)]
#[template(path = "transaction-history.html")]
struct TransactionHistoryTemplate {
    transactions: Vec<TransactionTemplate>,
}

#[derive(Clone)]
struct TransactionTemplate {
    alt_key: String,
    timestamp: i64,
    action: String,
}

pub async fn transaction_history() -> impl IntoResponse {
    let history = TransactionHistoryTemplate {
        transactions: vec![TransactionTemplate { alt_key: "asdfasdf".into(), timestamp: Utc::now().timestamp_micros(), action: "delete".into() }; 25]
    };

    (StatusCode::OK, Html(history.render().unwrap()))
}

pub async fn ui_main() -> impl IntoResponse {
    Html(MAIN_PAGE)
}