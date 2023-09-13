use actix_web::HttpResponse;
use actix_web::web::{Data, Json, Path, Query};
use uuid::Uuid;
use crate::data::db_layer::Filter;
use crate::data::Form;
use crate::logic::{AppState, Error};
use crate::logic::messages::{AddType, EditType, Internal, InternalMessage, RemoveType};

#[actix_web::post("/forms/submit/{template}")]
async fn submit_form(
    data: Data<AppState>,
    path: Path<String>,
    form: Json<Form>,
) -> Result<HttpResponse, Error> {

    let msg = Internal::Add(AddType::Form(
        form.0,
        path.into_inner(),
    ));

    let key = data.mutate(InternalMessage::new(msg)).await?;
    Ok(HttpResponse::Ok().body(key))
}

#[actix_web::delete("/forms/remove/template/{template}/id/{id}")]
async fn remove_form(
    data: Data<AppState>,
    path: Path<(String, Uuid)>
) -> Result<HttpResponse, Error> {
    let inner = path.into_inner();
    let msg = Internal::Remove(RemoveType::Form(
        inner.0,
        inner.1
    ));

    data.mutate(InternalMessage::new(msg)).await?;
    Ok(HttpResponse::Ok().finish())
}

#[actix_web::put("/forms/edit/template/{template}/id/{id}")]
async fn edit_form(
    data: Data<AppState>,
    path: Path<(String, Uuid)>,
    form: Json<Form>
) -> Result<HttpResponse, Error> {
    let inner = path.into_inner();
    let msg = Internal::Edit(EditType::Form(
        form.0,
        inner.1,
        inner.0
    ));

    let id = data.mutate(InternalMessage::new(msg)).await?;

    Ok(HttpResponse::Ok().body(id))
}

#[actix_web::get("/forms/get/template/{template}/id/{id}")]
async fn get_form(
    data: Data<AppState>,
    path: Path<(String, Uuid)>
) -> Result<HttpResponse, Error> {
    let inner = path.into_inner();
    let form = data.get_form_from_id(&inner.0, inner.1).await?;

    Ok(HttpResponse::Ok().json(form))
}

#[actix_web::get("/forms/get/template/{template}")]
async fn get_forms(
    data: Data<AppState>,
    path: Path<String>,
    query: Query<Filter>,
) -> Result<HttpResponse, Error> {
    let path_data = path.into_inner();
    let forms: Vec<(Form, Uuid)> = data.get(path_data, query.0).await?;

    Ok(HttpResponse::Ok().json(forms))
}