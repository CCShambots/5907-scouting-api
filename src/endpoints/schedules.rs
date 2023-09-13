use actix_web::http::StatusCode;
use actix_web::HttpResponse;
use actix_web::web::{Data, Json, Path, Query};
use uuid::Uuid;
use crate::data::db_layer::Filter;
use crate::data::{Form, Schedule};
use crate::logic::{AppState, Error};
use crate::logic::messages::{AddType, EditType, Internal, InternalMessage, RemoveType};

#[actix_web::post("/schedules/submit")]
async fn submit_schedule(
    data: Data<AppState>,
    schedule: Json<Schedule>
) -> Result<HttpResponse, Error> {
    let msg = Internal::Add(AddType::Schedule(schedule.0));
    let resp = data.mutate(InternalMessage::new(msg)).await?;

    Ok(HttpResponse::Ok().body(resp))
}

#[actix_web::delete("/schedules/{event}/remove")]
async fn remove_schedule(
    data: Data<AppState>,
    path: Path<String>
) -> Result<HttpResponse, Error> {
    let msg = Internal::Remove(RemoveType::Schedule(path.into_inner()));
    let resp = data.mutate(InternalMessage::new(msg)).await?;

    Ok(HttpResponse::Ok().body(resp))
}

#[actix_web::put("/schedules/edit")]
async fn edit_schedule(
    data: Data<AppState>,
    schedule: Json<Schedule>
) -> Result<HttpResponse, Error> {
    let msg = Internal::Edit(EditType::Schedule(schedule.0));
    let resp = data.mutate(InternalMessage::new(msg)).await?;

    Ok(HttpResponse::Ok().body(resp))
}

#[actix_web::get("/schedules")]
async fn get_schedules(
    data: Data<AppState>
) -> Result<HttpResponse, Error> {
    todo!()
}

#[actix_web::get("/schedules/{event}")]
async fn get_schedule(
    data: Data<AppState>,
    path: Path<String>
) -> Result<HttpResponse, Error> {
    todo!()
}