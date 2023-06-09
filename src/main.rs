mod data;
mod state;

use std::sync::Arc;
use actix_web::{App, HttpServer, web, Result, main, http, Responder, HttpResponse, ResponseError};
use actix_web::web::{Data, Json, Path, Query};
use actix_cors::Cors;
use tokio::fs::read_to_string;
use crate::data::Form;
use crate::data::template::{FieldDataType, FormTemplate};
use crate::state::{AppState, GetError, SubmitError};

#[main]
async fn main() -> std::io::Result<()> {
    run_server().await
}

async fn run_server() -> std::io::Result<()> {
    let db = sled::open("database")?;
    let state = AppState::new(db);

    println!("Building Cache...");

    state.build_cache().await.unwrap();

    println!("Done Building Cache, Starting HTTP Server");

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
            .service(event_names)
            .service(event)
            .service(teams)
            .service(game_configs)
            .service(submit_form)
    })
        .bind(("0.0.0.0", 8080))?
        .run()
        .await
}

async fn get_default(def: &str) -> Result<String> {
    Ok(minimize(read_to_string(format!("defaults/{def}.json")).await?))
}

fn minimize(s: String) -> String {
    s.replace("", "").replace("\t", "").replace("\n", "")
}

#[actix_web::post("/template/{template}/submit")]
async fn submit_form(data: Data<AppState>, path: Path<String>, form: Json<Form>) -> Result<HttpResponse, SubmitError> {
    data.submit_form(path.into_inner(), &form.0).await?;

    Ok(HttpResponse::Ok().finish())
}
#[actix_web::get("/template/{template}/teams/{team}")]
async fn teams(data: Data<AppState>, path: Path<(String, i64)>) -> Result<HttpResponse, GetError> {
    let path_data = path.into_inner();
    let forms: Vec<Form> = data.get_all_for_team(path_data.0, path_data.1).await?;

    Ok(HttpResponse::Ok().json(forms))
}

//TODO: change these VV
#[actix_web::get("/event/{event}")]
async fn event(path: Path<(String)>) -> Result<String> {
    get_default("event").await
}

#[actix_web::get("/event/event-names")]
async fn event_names() -> Result<String> {
    get_default("event-names").await
}



#[actix_web::get("/game-configs")]
async fn game_configs() -> Result<String> {
    Ok(read_to_string("../templates/2023_CHARGED_UP.json").await?)
}