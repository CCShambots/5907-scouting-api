pub mod forms;
pub mod templates;
pub mod schedules;
pub mod scouters;
mod bytes;

use actix_web::HttpResponse;

#[actix_web::get("/status")]
async fn status() -> HttpResponse {
    HttpResponse::Ok().finish()
}
