use actix_web::HttpResponse;
use actix_web::web::{Data, Json, Path};
use crate::data::{Schedule};
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

#[actix_web::delete("/schedules/remove/event/{event}")]
async fn remove_schedule(
    data: Data<AppState>,
    path: Path<String>
) -> Result<HttpResponse, Error> {
    let msg = Internal::Remove(RemoveType::Schedule(path.into_inner()));
    let resp = data.mutate(InternalMessage::new(msg)).await?;

    Ok(HttpResponse::Ok().body(resp))
}

#[actix_web::put("/schedules/get/edit")]
async fn edit_schedule(
    data: Data<AppState>,
    schedule: Json<Schedule>
) -> Result<HttpResponse, Error> {
    let msg = Internal::Edit(EditType::Schedule(schedule.0));
    let resp = data.mutate(InternalMessage::new(msg)).await?;

    Ok(HttpResponse::Ok().body(resp))
}

#[actix_web::get("/schedules/get")]
async fn get_schedules(
    data: Data<AppState>
) -> Result<HttpResponse, Error> {
    Ok(HttpResponse::Ok().json(data.get_schedule_events().await?))
}

#[actix_web::get("/schedules/get/event/{event}")]
async fn get_schedule(
    data: Data<AppState>,
    path: Path<String>
) -> Result<HttpResponse, Error> {
    let resp = data.get_schedule(&path.into_inner()).await?;

    Ok(HttpResponse::Ok().json(resp))
}