mod data;
mod settings;
mod logic;

use crate::data::db_layer::{Filter};
use crate::data::{Form, Schedule};
use crate::settings::Settings;
use crate::logic::{AppState, Error};
use actix_cors::Cors;
use actix_web::web::{Data, Json, Path, Query};
use actix_web::{http, main, App, HttpResponse, HttpServer, Result};
use sled::{Config, Mode};
use uuid::Uuid;
use crate::data::template::FormTemplate;
use crate::logic::messages::{AddFormData, FormMessage, Internal, InternalMessage, RemoveFormData, ScheduleMessage, TemplateMessage};

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
            .service(delete_form)
            .service(delete_schedule)
            .service(delete_template)
            .service(modify_template)
            .service(add_template)
            .service(status)
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}

#[actix_web::delete("/template/delete/{template}")]
async fn delete_template(
    data: Data<AppState>,
    path: Path<String>
) -> Result<HttpResponse, Error> {
    let msg = Internal::Template(TemplateMessage::Remove(
        path.into_inner()
    ));

    data.mutate(InternalMessage::new(msg)).await?;
    Ok(HttpResponse::Ok().finish())
}

#[actix_web::post("/template/modify")]
async fn modify_template(
    data: Data<AppState>,
    template: Json<FormTemplate>
) -> Result<HttpResponse, Error> {
    let msg = Internal::Template(TemplateMessage::Modify(
        template.into_inner()
    ));

    data.mutate(InternalMessage::new(msg)).await?;
    Ok(HttpResponse::Ok().finish())
}

#[actix_web::post("/template/add")]
async fn add_template(
    data: Data<AppState>,
    template: Json<FormTemplate>
) -> Result<HttpResponse, Error> {
    let msg = Internal::Template(TemplateMessage::Add(
        template.into_inner()
    ));

    data.mutate(InternalMessage::new(msg)).await?;
    Ok(HttpResponse::Ok().finish())
}

#[actix_web::post("/template/{template}/submit")]
async fn submit_form(
    data: Data<AppState>,
    path: Path<String>,
    form: Json<Vec<Form>>,
) -> Result<HttpResponse, Error> {
    let msg = Internal::Form(FormMessage::Add(
        AddFormData {
            template: path.into_inner(),
            forms: form.0,
        }
    ));

    data.mutate(InternalMessage::new(msg)).await?;
    Ok(HttpResponse::Ok().finish())
}

#[actix_web::delete("/template/{template}/delete/{id}")]
async fn delete_form(
    data: Data<AppState>,
    path: Path<(String, Uuid)>
) -> Result<HttpResponse, Error> {
    let inner = path.into_inner();
    let msg = Internal::Form(FormMessage::Remove(
        RemoveFormData {
            template: inner.0,
            id: inner.1
        }
    ));

    data.mutate(InternalMessage::new(msg)).await?;
    Ok(HttpResponse::Ok().finish())
}

#[actix_web::get("/status")]
async fn status() -> HttpResponse {
    HttpResponse::Ok().finish()
}

#[actix_web::post("/schedules/{event}/submit")]
async fn set_schedule(
    data: Data<AppState>,
    schedule: Json<Schedule>,
) -> Result<HttpResponse, Error> {
    let msg = Internal::Schedule(ScheduleMessage::Modify(
        schedule.into_inner()
    ));

    data.mutate(InternalMessage::new(msg)).await?;
    Ok(HttpResponse::Ok().finish())
}

#[actix_web::delete("/schedules/{event}/delete")]
async fn delete_schedule(
    data: Data<AppState>,
    path: Path<String>
) -> Result<HttpResponse, Error> {
    let msg = Internal::Schedule(ScheduleMessage::Remove(
        path.into_inner()
    ));

    data.mutate(InternalMessage::new(msg)).await?;
    Ok(HttpResponse::Ok().finish())
}

#[actix_web::get("/template/{template}/get")]
async fn teams(
    data: Data<AppState>,
    path: Path<String>,
    query: Query<Filter>,
) -> Result<HttpResponse, Error> {
    let path_data = path.into_inner();
    let forms: Vec<Form> = data.get(path_data, query.0).await?;

    Ok(HttpResponse::Ok().json(forms))
}

#[actix_web::get("/templates/{template}")]
async fn get_template(data: Data<AppState>, path: Path<String>) -> Result<HttpResponse, Error> {
    Ok(HttpResponse::Ok().json(data.get_template(&path.into_inner()).await?))
}

#[actix_web::get("/templates")]
async fn templates(data: Data<AppState>) -> Result<HttpResponse, Error> {
    Ok(HttpResponse::Ok().json(data.get_templates().await))
}

#[actix_web::get("/schedules/{event}/{scouter}")]
async fn get_shifts(
    data: Data<AppState>,
    path: Path<(String, String)>,
) -> Result<HttpResponse, Error> {
    let path_data = path.into_inner();
    Ok(HttpResponse::Ok().json(data.get_shifts(&path_data.0, &path_data.1).await?))
}

#[actix_web::get("/schedules/{event}")]
async fn get_schedule(data: Data<AppState>, path: Path<String>) -> Result<HttpResponse, Error> {
    Ok(HttpResponse::Ok().json(data.get_schedule(&path.into_inner()).await?))
}
