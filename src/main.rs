mod data;
mod settings;
mod state;

use crate::data::db_layer::{Filter, GetError, SubmitError};
use crate::data::{Form, Schedule};
use crate::settings::Settings;
use crate::state::AppState;
use actix_cors::Cors;
use actix_web::web::{Data, Json, Path, Query};
use actix_web::{http, main, App, HttpResponse, HttpServer, Result};
use sled::{Config, Mode};

#[main]
async fn main() -> std::io::Result<()> {
    run_server().await
}

async fn run_server() -> std::io::Result<()> {
    let config = Settings::new("config.toml").unwrap();

    println!("Configuration: {:?}", config);

    let db = Config::default()
        .path(config.database.path.clone())
        .mode(Mode::HighThroughput)
        .cache_capacity(config.database.cache_capacity)
        .open()?;

    println!(
        "{} MB",
        db.size_on_disk().unwrap() as f64 / (1024.0 * 1024.0)
    );

    let state = AppState::new(db, config);

    println!("Building Cache");

    state.build_cache().await.unwrap();

    println!("Finished building cache, starting http server...");

    let data = Data::new(state);

    HttpServer::new(move || {
        let cors = Cors::default()
            .allow_any_origin() // <--- this // ?
            .allowed_methods(vec!["GET", "POST"])
            .allowed_headers(vec![http::header::AUTHORIZATION, http::header::ACCEPT])
            .allowed_header(http::header::CONTENT_TYPE)
            .max_age(3600);

        App::new()
            .app_data(data.clone())
            .wrap(cors)
            .service(teams)
            .service(submit_form)
            .service(get_template)
            .service(templates)
            .service(set_schedule)
            .service(get_schedule)
            .service(get_shifts)
            .service(status)
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}

#[actix_web::post("/template/{template}/submit")]
async fn submit_form(
    data: Data<AppState>,
    path: Path<String>,
    form: Json<Form>,
) -> Result<HttpResponse, SubmitError> {
    data.submit_form(path.into_inner(), &form.0).await?;

    Ok(HttpResponse::Ok().finish())
}

#[actix_web::post("/status")]
async fn status() -> HttpResponse {
    HttpResponse::Ok().finish()
}

#[actix_web::post("/schedules/{event}/submit")]
async fn set_schedule(
    data: Data<AppState>,
    schedule: Json<Schedule>,
    path: Path<String>
) -> Result<HttpResponse, SubmitError> {
    data.set_schedule(path.into_inner(), schedule.into_inner())
        .await?;

    Ok(HttpResponse::Ok().finish())
}

#[actix_web::get("/template/{template}/get")]
async fn teams(
    data: Data<AppState>,
    path: Path<String>,
    query: Query<Filter>,
) -> Result<HttpResponse, GetError> {
    let path_data = path.into_inner();
    let forms: Vec<Form> = data.get(path_data, query.0).await?;

    Ok(HttpResponse::Ok().json(forms))
}

#[actix_web::get("/templates/{template}")]
async fn get_template(data: Data<AppState>, path: Path<String>) -> Result<HttpResponse, GetError> {
    Ok(HttpResponse::Ok().json(data.get_template(path.into_inner()).await?))
}

#[actix_web::get("/templates")]
async fn templates(data: Data<AppState>) -> Result<HttpResponse, GetError> {
    Ok(HttpResponse::Ok().json(data.get_templates().await))
}

#[actix_web::get("/schedules/{event}/{scouter}")]
async fn get_shifts(
    data: Data<AppState>,
    path: Path<(String, String)>,
) -> Result<HttpResponse, GetError> {
    let path_data = path.into_inner();
    Ok(HttpResponse::Ok().json(data.get_shifts(path_data.0, path_data.1).await?))
}

#[actix_web::get("/schedules/{event}")]
async fn get_schedule(data: Data<AppState>, path: Path<String>) -> Result<HttpResponse, GetError> {
    Ok(HttpResponse::Ok().json(data.get_schedule(path.into_inner()).await?))
}
