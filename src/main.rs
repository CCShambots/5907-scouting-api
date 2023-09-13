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

            .service(endpoints::templates::get_template)
            .service(endpoints::templates::get_templates)
            .service(endpoints::templates::submit_template)
            .service(endpoints::templates::edit_template)
            .service(endpoints::templates::remove_template)

            .service(endpoints::schedules::get_schedule)
            .service(endpoints::schedules::get_schedules)
            .service(endpoints::schedules::submit_schedule)
            .service(endpoints::schedules::edit_schedule)
            .service(endpoints::schedules::remove_schedule)

            .service(endpoints::scouters::get_scouter)
            .service(endpoints::scouters::get_scouters)
            .service(endpoints::scouters::submit_scouter)
            .service(endpoints::scouters::edit_scouter)
            .service(endpoints::scouters::remove_scouter)

            .service(endpoints::bytes::get_bytes)
            .service(endpoints::bytes::get_bytes_by_key)
            .service(endpoints::bytes::submit_bytes)
            .service(endpoints::bytes::edit_bytes)
            .service(endpoints::bytes::remove_bytes)
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}
