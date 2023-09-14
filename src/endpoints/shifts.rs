use actix_web::HttpResponse;
use actix_web::web::{Bytes, Data, Json, Path};
use crate::data::{Schedule, Shift};
use crate::logic::{AppState, Error};
use crate::logic::messages::{AddType, EditType, Internal, InternalMessage, RemoveType};

#[actix_web::post("/shifts/submit/event/{event}")]
async fn submit_shift(
    data: Data<AppState>,
    path: Path<String>,
    shift: Json<Shift>
) -> Result<HttpResponse, Error> {
    let msg = Internal::Add(AddType::Shift(
        shift.0,
        path.into_inner()
    ));
    let resp = data.mutate(InternalMessage::new(msg)).await?;

    Ok(HttpResponse::Ok().body(resp))
}

#[actix_web::delete("/shifts/remove/event/{event}/index/{idx}")]
async fn remove_shift(
    data: Data<AppState>,
    path: Path<(String, u64)>,
) -> Result<HttpResponse, Error> {
    let inner = path.into_inner();
    let msg = Internal::Remove(RemoveType::Shift(
        inner.0,
        inner.1
    ));
    let resp = data.mutate(InternalMessage::new(msg)).await?;

    Ok(HttpResponse::Ok().body(resp))
}

#[actix_web::get("/shifts/get")]
async fn get_shifts(
    data: Data<AppState>
) -> Result<HttpResponse, Error> {
    Ok(HttpResponse::Ok().json(data.get_shifts().await?))
}

#[actix_web::get("/shifts/get/event/{event}/index/{idx}")]
async fn get_shift(
    data: Data<AppState>,
    path: Path<(String, u64)>
) -> Result<HttpResponse, Error> {
    let inner = path.into_inner();
    Ok(HttpResponse::Ok().json(data.get_shift(&inner.0, inner.1).await?))
}