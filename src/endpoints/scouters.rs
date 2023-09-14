use actix_web::HttpResponse;
use actix_web::web::{Data, Json, Path};
use crate::data::{Schedule, Scouter};
use crate::logic::{AppState, Error};
use crate::logic::messages::{AddType, EditType, Internal, InternalMessage, RemoveType};

#[actix_web::post("/scouters/submit")]
async fn submit_scouter(
    data: Data<AppState>,
    scouter: Json<Scouter>
) -> Result<HttpResponse, Error> {
    let msg = Internal::Add(AddType::Scouter(scouter.0));
    let resp = data.mutate(InternalMessage::new(msg)).await?;

    Ok(HttpResponse::Ok().body(resp))
}

#[actix_web::delete("/scouters/remove/scouterkey/{key}")]
async fn remove_scouter(
    data: Data<AppState>,
    path: Path<String>
) -> Result<HttpResponse, Error> {
    let msg = Internal::Remove(RemoveType::Scouter(path.into_inner()));
    let resp = data.mutate(InternalMessage::new(msg)).await?;

    Ok(HttpResponse::Ok().body(resp))
}

#[actix_web::put("/scouters/edit")]
async fn edit_scouter(
    data: Data<AppState>,
    scouter: Json<Scouter>
) -> Result<HttpResponse, Error> {
    let msg = Internal::Edit(EditType::Scouter(scouter.0));
    let resp = data.mutate(InternalMessage::new(msg)).await?;

    Ok(HttpResponse::Ok().body(resp))
}

#[actix_web::get("/scouters/get")]
async fn get_scouters(
    data: Data<AppState>
) -> Result<HttpResponse, Error> {
    Ok(HttpResponse::Ok().json(data.get_scouters().await?))
}

#[actix_web::get("/scouters/get/scouterkey/{key}")]
async fn get_scouter(
    data: Data<AppState>,
    key: Path<String>
) -> Result<HttpResponse, Error> {
    Ok(HttpResponse::Ok().json(data.get_scouter(&key.into_inner()).await?))
}