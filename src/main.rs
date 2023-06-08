mod data;

use actix_web::{App, HttpServer, web, Result, main, http};
use actix_web::web::Path;
use actix_cors::Cors;
use tokio::fs::read_to_string;

#[main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| {

        let cors = Cors::default()
            .allow_any_origin() // <--- this
            .allowed_methods(vec!["GET", "POST"])
            .allowed_headers(vec![http::header::AUTHORIZATION, http::header::ACCEPT])
            .allowed_header(http::header::CONTENT_TYPE)
            .max_age(3600);

        App::new()
            .wrap(cors)
            .service(event_names)
            .service(event)
            .service(teams)
            .service(game_configs)
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

#[actix_web::get("/event/{event}")]
async fn event(path: Path<(String)>) -> Result<String> {
    get_default("event").await
}

#[actix_web::get("/event/event-names")]
async fn event_names() -> Result<String> {
    get_default("event-names").await
}

#[actix_web::get("/teams/{team}")]
async fn teams(path: Path<(u16)>) -> Result<String> {
    get_default("teams").await
}

#[actix_web::get("/game-configs")]
async fn game_configs() -> Result<String> {
    get_default("game-configs").await
}