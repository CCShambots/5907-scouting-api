use actix_web::HttpResponse;
use actix_web::web::{Data, Json, Path};
use crate::data::template::FormTemplate;
use crate::logic::{AppState, Error};
use crate::logic::messages::{AddType, EditType, Internal, InternalMessage, RemoveType};


#[actix_web::post("/templates/submit")]
async fn submit_template(
    data: Data<AppState>,
    template: Json<FormTemplate>
) -> Result<HttpResponse, Error> {
    let msg = Internal::Add(AddType::Template(template.0));
    let resp = data.mutate(InternalMessage::new(msg)).await?;

    Ok(HttpResponse::Ok().body(resp))
}

#[actix_web::put("/templates/edit")]
async fn edit_template(
    data: Data<AppState>,
    template: Json<FormTemplate>
) -> Result<HttpResponse, Error> {
    let msg = Internal::Edit(EditType::Template(template.0));
    let resp = data.mutate(InternalMessage::new(msg)).await?;

    Ok(HttpResponse::Ok().body(resp))
}

#[actix_web::delete("/templates/remove/name/{name}")]
async fn remove_template(
    data: Data<AppState>,
    path: Path<String>
) -> Result<HttpResponse, Error> {
    let msg = Internal::Remove(RemoveType::Template(path.into_inner()));
    let resp = data.mutate(InternalMessage::new(msg)).await?;

    Ok(HttpResponse::Ok().body(resp))
}

#[actix_web::get("/templates/get")]
async fn get_templates(
    data: Data<AppState>
) -> Result<HttpResponse, Error> {
    let resp = data.get_templates().await;

    Ok(HttpResponse::Ok().json(resp))
}

#[actix_web::get("/templates/get/name/{name}")]
async fn get_template(
    data: Data<AppState>,
    path: Path<String>
) -> Result<HttpResponse, Error> {
    let resp = data.get_template(&path.into_inner()).await?;

    Ok(HttpResponse::Ok().json(resp))
}
