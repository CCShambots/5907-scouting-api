pub mod messages;

use std::fmt::{Debug, Display, Formatter};
use actix_web::http::StatusCode;
use actix_web::{HttpResponse, ResponseError};
use actix_web::body::BoxBody;
use actix_web::http::header::ContentType;
use derive_more::{Display, Error};
use crate::data::db_layer::{DBLayer, Filter, GetError, SubmitError};
use crate::data::template::FormTemplate;
use crate::data::{Form, Schedule, Shift};
use crate::settings::Settings;
use sled::Db;
use uuid::Uuid;
use crate::logic::messages::{AddFormData, FormMessage, Internal, InternalMessage, ScheduleMessage, ScouterMessage, TemplateMessage};

impl AppState {
    pub fn new(db: Db, config: Settings) -> Self {
        Self {
            db_layer: DBLayer::new(db, "templates"),
            config,
        }
    }

    pub async fn mutate(&self, message: InternalMessage) -> Result<(), Error> {
        match message.msg {
            Internal::Form(msg) =>
                self.handle_form_message(msg).await,

            Internal::Template(msg) =>
                self.handle_template_message(msg).await,

            Internal::Scouter(msg) =>
                self.handle_scouter_message(msg).await,

            Internal::Schedule(msg) =>
                self.handle_schedule_message(msg).await
        }
    }

    async fn handle_form_message(&self, message: FormMessage) -> Result<(), Error> {
        //erm what the freak

        match message {
            FormMessage::Add(data) => self.submit_forms(data).await,
            FormMessage::Remove(data) => self.db_layer.remove_form(data.template, data.id).await
        }.map_err(|err| err.into())
    }

    async fn handle_template_message(&self, message: TemplateMessage) -> Result<(), Error> {
        todo!()
    }

    async fn handle_scouter_message(&self, message: ScouterMessage) -> Result<(), Error> {
        todo!()
    }

    async fn handle_schedule_message(&self, message: ScheduleMessage) -> Result<(), Error> {
        todo!()
    }

    pub async fn get_templates(&self) -> Vec<String> {
        self.db_layer.get_templates().await
    }

    pub async fn get_template(&self, template: String) -> Result<FormTemplate, GetError> {
        self.db_layer.get_template(template).await
    }

    pub async fn build_cache(&self) -> Result<(), GetError> {
        self.db_layer.build_cache().await
    }

    pub async fn get(&self, template: String, filter: Filter) -> Result<Vec<Form>, GetError> {
        self.db_layer.get(template, filter).await
    }

    async fn submit_forms(&self, form: AddFormData) -> Result<(), SubmitError> {
        self.db_layer.submit_forms(form.template, form.forms).await
    }

    pub async fn get_schedule(&self, event: String) -> Result<Schedule, GetError> {
        self.db_layer.get_schedule(event).await
    }

    pub async fn set_schedule(&self, event: String, schedule: Schedule) -> Result<(), SubmitError> {
        self.db_layer.set_schedule(event, schedule).await
    }

    pub async fn get_shifts(&self, event: String, scouter: String) -> Result<Vec<Shift>, GetError> {
        self.db_layer.get_shifts(event, scouter).await
    }
}

pub struct AppState {
    db_layer: DBLayer,
    config: Settings,
}

impl ResponseError for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::Internal => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::BAD_REQUEST
        }
    }

    fn error_response(&self) -> HttpResponse<BoxBody> {
        HttpResponse::build(self.status_code())
            .insert_header(ContentType::json())
            .body(self.to_string())
    }
}

impl From<SubmitError> for Error {
    fn from(value: SubmitError) -> Self {
        match value {
            SubmitError::Internal => Self::Internal,
            SubmitError::FormDoesNotFollowTemplate { requested_template } =>
                Self::FormDoesNotFollowTemplate { template: requested_template },
            SubmitError::TemplateDoesNotExist { requested_template } =>
                Self::TemplateDoesNotExist { template: requested_template }
        }
    }
}

#[derive(Debug, Display, Error)]
pub enum Error {
    Internal,
    UuidDoesNotExist { uuid: Uuid },
    TemplateDoesNotExist { template: String } ,
    ScouterDoesNotExist { scouter: String } ,
    ScheduleDoesNotExist { schedule: String },
    TemplateNameReserved { name: String },
    FormDoesNotFollowTemplate { template: String }
}
