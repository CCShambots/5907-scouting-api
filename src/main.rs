use crate::storage_manager::StorageManager;
use crate::sync::{ChildID, SyncManager};
use auth::{GoogleAuthenticator, GoogleUser, JwtManagerBuilder};
use axum::body::Body;
use axum::http::Method;
use axum::middleware::from_extractor;
use axum::response::{IntoResponse, Response};
use axum::Extension;
use axum_server::tls_rustls::RustlsConfig;
use jwt_simple::prelude::*;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::{RandomIdGenerator, Sampler};
use opentelemetry_sdk::{trace, Resource};
use sqlx::migrate::MigrateDatabase;
use sqlx::{Connection, Executor, Sqlite, SqliteConnection};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs;
use tower::ServiceBuilder;
use tower_http::compression::CompressionLayer;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::{info, instrument, warn};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use uuid::Uuid;

mod auth;
mod bytes;
mod datatypes;
mod forms;
mod misc;
mod schedules;
mod storage_manager;
mod sync;
mod templates;
mod transactions;

#[instrument(ret)]
async fn handler(user_info: GoogleUser) -> Result<ApiResponse, ApiError> {
    Ok(ApiResponse::OK(user_info.email))
}

#[derive(Debug)]
enum ApiResponse {
    OK(String),
}

impl IntoResponse for ApiResponse {
    fn into_response(self) -> Response {
        match self {
            Self::OK(x) => Response::new(Body::from(x)),
        }
    }
}

#[derive(Debug)]
enum ApiError {}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        todo!()
    }
}

#[derive(Deserialize)]
struct TlsConfig {
    key_path: String,
    cert_path: String,
    metrics_bind: String,
    application_bind: String,
}

#[instrument]
async fn init_storage(path: &str) -> Result<(), anyhow::Error> {
    let formatted_path = format!("sqlite://{path}database.db");

    if !Sqlite::database_exists(&formatted_path).await? {
        warn!("No database found at specified location [{path}], initializing new one");
        Sqlite::create_database(&formatted_path).await?;
    }

    if !fs::try_exists(format!("{}blobs/", path)).await? {
        warn!("No blob folder found at specified location [{path}], initializing new one");
        fs::create_dir(format!("{}blobs/", path)).await?;
    }

    let mut conn = SqliteConnection::connect(&formatted_path).await?;

    if sqlx::query(&format!(
        "SELECT COUNT(*) FROM {}",
        storage_manager::TRANSACTION_TABLE
    ))
        .fetch_one(&mut conn)
        .await
        .is_err()
    {
        warn!("{} table not found, creating new one", storage_manager::TRANSACTION_TABLE);

        sqlx::query(&format!(
            "CREATE TABLE {} ( \
            id TEXT NOT NULL, \
            data_type TEXT NOT NULL, \
            action TEXT NOT NULL, \
            blob_id TEXT NOT NULL, \
            alt_key TEXT NOT NULL, \
            timestamp INTEGER NOT NULL\
            )",
            storage_manager::TRANSACTION_TABLE
        ))
            .execute(&mut conn)
            .await?;

        sqlx::query(&format!(
            "CREATE INDEX idx_timestamp \
            ON {} (timestamp DESC)",
            storage_manager::TRANSACTION_TABLE
        ))
            .execute(&mut conn)
            .await?;

        info!("{} table created", storage_manager::TRANSACTION_TABLE);
    }

    if sqlx::query(&format!(
        "SELECT COUNT(*) FROM {}",
        storage_manager::FORMS_TABLE
    ))
        .fetch_one(&mut conn)
        .await
        .is_err()
    {
        warn!("{} table not found, creating new one", storage_manager::FORMS_TABLE);

        sqlx::query(&format!(
            "CREATE TABLE {} ( \
            blob_id TEXT NOT NULL, \
            team INTEGER NOT NULL, \
            match_number INTEGER NOT NULL, \
            event_key TEXT NOT NULL, \
            template TEXT NOT NULL\
            )",
            storage_manager::FORMS_TABLE
        ))
            .execute(&mut conn)
            .await?;

        info!("{} table created", storage_manager::FORMS_TABLE);
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    setup_tracing();

    let settings = config::Config::builder()
        .add_source(config::File::with_name("settings"))
        .build()
        .unwrap();

    let tls_config = settings
        .get::<TlsConfig>("tls_config")
        .expect("No TLS config found");

    let path: String = settings.get("path").expect("No storage path config found");

    init_storage(&path)
        .await
        .expect("Failed to initialize storage");

    let google_authenticator = settings
        .get::<GoogleAuthenticator>("authenticator")
        .expect("No authenticator config found");

    let jwt_manager = settings
        .get::<JwtManagerBuilder>("jwt_manager")
        .expect("No JWT config found")
        .build();

    let sync_manager = settings
        .get::<SyncManager>("sync")
        .expect("No sync config found");

    let storage_manager = StorageManager::new(&path)
        .await
        .expect("Failed to create storage manager (probably issue with sqlite db)");


    // set up metrics for adding into the application
    let metrics = axum_otel_metrics::HttpMetricsLayerBuilder::new().build();
    // get the /metrics endpoint for publishing
    let metrics_routes = metrics.routes();

    // set up the routes and middleware
    let router = axum::Router::new()
        //.route("/protected/age/*path", axum::routing::get(misc::age))
        .route("/protected", axum::routing::get(handler))
        .route("/protected/code", axum::routing::get(auth::auth_code))
        //bytes
        .route("/protected/bytes/", axum::routing::get(bytes::list_bytes))
        .route(
            "/protected/bytes/:blob_id",
            axum::routing::post(bytes::store_bytes),
        )
        .route(
            "/protected/bytes/:blob_id",
            axum::routing::get(bytes::get_bytes),
        )
        .route(
            "/protected/bytes/:blob_id",
            axum::routing::delete(bytes::delete_bytes),
        )
        .route(
            "/protected/bytes/:blob_id",
            axum::routing::patch(bytes::edit_bytes),
        )
        //templates
        .route(
            "/protected/templates/",
            axum::routing::get(templates::list_templates),
        )
        .route(
            "/protected/template/:template",
            axum::routing::get(templates::get_template),
        )
        .route(
            "/protected/template/",
            axum::routing::patch(templates::edit_template),
        )
        .route(
            "/protected/template/:template",
            axum::routing::delete(templates::delete_template),
        )
        .route(
            "/protected/template/",
            axum::routing::post(templates::add_template),
        )
        //schedules
        .route(
            "/protected/schedules/",
            axum::routing::get(schedules::list_schedules),
        )
        .route(
            "/protected/schedule/:schedule",
            axum::routing::get(schedules::get_schedule),
        )
        .route(
            "/protected/schedule/",
            axum::routing::patch(schedules::edit_schedule),
        )
        .route(
            "/protected/schedule/:schedule",
            axum::routing::delete(schedules::delete_schedule),
        )
        .route(
            "/protected/schedule/",
            axum::routing::post(schedules::add_schedule),
        )
        //forms
        .route(
            "/protected/forms/:template/ids",
            axum::routing::get(forms::list_forms),
        )
        .route(
            "/protected/forms/:template/",
            axum::routing::get(forms::filter_forms),
        )
        .route(
            "/protected/form/:template/:id",
            axum::routing::get(forms::get_form),
        )
        .route(
            "/protected/form/:template/:id",
            axum::routing::patch(forms::edit_form),
        )
        .route(
            "/protected/form/:template/:id",
            axum::routing::delete(forms::delete_form),
        )
        .route(
            "/protected/form/:template",
            axum::routing::post(forms::add_form),
        )
        .layer(from_extractor::<GoogleUser>())
        .route("/", axum::routing::get(auth::login_handler))
        .route(
            "/auth/:code/:email",
            axum::routing::get(auth::get_jwt_cache_from_code),
        )
        //Sync
        /*.route("/sync/diff", axum::routing::get(sync::diff))
        .route_layer(from_extractor::<ChildID>())*/
        .layer(CorsLayer::very_permissive())
        .layer(
            ServiceBuilder::new()
                .layer(Extension(Arc::new(google_authenticator)))
                .layer(Extension(Arc::new(storage_manager)))
                .layer(Extension(Arc::new(jwt_manager)))
                .layer(Extension(Arc::new(sync_manager)))
                .layer(metrics)
                .layer(CompressionLayer::new())
                .layer(TraceLayer::new_for_http()),
        );

    info!("Starting http server");

    // Run the application with TLS
    let ssl_config = RustlsConfig::from_pem_file(tls_config.cert_path, tls_config.key_path)
        .await
        .expect("Could not get ssl cert");
    tokio::spawn(async move {
        axum_server::bind_rustls(tls_config.application_bind.parse().unwrap(), ssl_config)
            .serve(router.into_make_service())
            .await
            .unwrap()
    });

    info!("starting metrics server");

    // Metrics endpoint should be published on a non-TLS port separately
    axum_server::bind(tls_config.metrics_bind.parse().unwrap())
        .serve(metrics_routes.into_make_service())
        .await
        .unwrap();
}

fn setup_tracing() {
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint("http://localhost:4317") // grafana agent endpoint
                .with_timeout(Duration::from_secs(3)),
        )
        .with_trace_config(
            trace::config()
                .with_sampler(Sampler::AlwaysOn) // this should be changed in high throughput settings
                .with_id_generator(RandomIdGenerator::default())
                .with_max_events_per_span(64)
                .with_max_attributes_per_span(16)
                .with_max_events_per_span(16)
                .with_resource(Resource::new(vec![KeyValue::new(
                    "service.name",
                    "example", // what the service name the metrics and traces are attached to
                )])),
        )
        .install_batch(opentelemetry_sdk::runtime::Tokio)
        .unwrap();
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("tower_http=trace,info")) // logging levels
        .with(tracing_subscriber::fmt::layer())
        //.with(tracing_opentelemetry::layer().with_tracer(tracer))
        .try_init()
        .unwrap();
}
