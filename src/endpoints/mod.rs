pub mod forms;

use actix_web::HttpResponse;

#[actix_web::get("/status")]
async fn status() -> HttpResponse {
    HttpResponse::Ok().finish()
}
