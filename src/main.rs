mod data;
mod settings;
mod logic;
mod endpoints;

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
use crate::logic::messages::{Internal, InternalMessage};

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
            .allowed_methods(vec!["GET", "POST", "DELETE", "PUT"])
            .allowed_headers(vec![http::header::AUTHORIZATION, http::header::ACCEPT])
            .allowed_header(http::header::CONTENT_TYPE)
            .max_age(3600);

        App::new()
            .app_data(data.clone())
            .wrap(cors)
            .service(endpoints::status)

            .service(endpoints::forms::get_form)
            .service(endpoints::forms::get_forms)
            .service(endpoints::forms::submit_form)
            .service(endpoints::forms::edit_form)
            .service(endpoints::forms::remove_form)

            .service(get_template)
            .service(templates)
            .service(set_schedule)
            .service(get_schedule)
            .service(get_shifts)
            .service(delete_schedule)
            .service(delete_template)
            .service(modify_template)
            .service(add_template)
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
