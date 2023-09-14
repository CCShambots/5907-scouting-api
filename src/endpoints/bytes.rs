use actix_web::HttpResponse;
use actix_web::web::{Bytes, Data, Json, Path};
use crate::data::{Schedule};
use crate::logic::{AppState, Error};
use crate::logic::messages::{AddType, EditType, Internal, InternalMessage, RemoveType};

#[actix_web::post("/bytes/submit/key/{key}")]
async fn submit_bytes(
    data: Data<AppState>,
    bytes: Bytes,
    path: Path<String>
) -> Result<HttpResponse, Error> {
    let msg = Internal::Add(AddType::Bytes(
        bytes.to_vec(),
        path.into_inner()
    ));
    let resp = data.mutate(InternalMessage::new(msg)).await?;

    Ok(HttpResponse::Ok().body(resp))
}

#[actix_web::delete("/bytes/remove/key/{key}")]
async fn remove_bytes(
    data: Data<AppState>,
    path: Path<String>
) -> Result<HttpResponse, Error> {
    let msg = Internal::Remove(RemoveType::Bytes(path.into_inner()));
    let resp = data.mutate(InternalMessage::new(msg)).await?;

    Ok(HttpResponse::Ok().body(resp))
}

#[actix_web::put("/bytes/edit/key/{key}")]
async fn edit_bytes(
    data: Data<AppState>,
    bytes: Bytes,
    path: Path<String>
) -> Result<HttpResponse, Error> {
    let msg = Internal::Edit(EditType::Bytes(
        bytes.to_vec(),
        path.into_inner()
    ));
    let resp = data.mutate(InternalMessage::new(msg)).await?;

    Ok(HttpResponse::Ok().body(resp))
}

#[actix_web::get("/bytes/get")]
async fn get_bytes(
    data: Data<AppState>
) -> Result<HttpResponse, Error> {
    Ok(HttpResponse::Ok().json(data.get_bytes().await?))
}

#[actix_web::get("/bytes/get/key/{key}")]
async fn get_bytes_by_key(
    data: Data<AppState>,
    path: Path<String>
) -> Result<HttpResponse, Error> {
    Ok(HttpResponse::Ok().body(data.get_bytes_by_key(&path.into_inner()).await?))
}