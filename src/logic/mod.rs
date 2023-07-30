pub mod messages;

use std::fmt::{Debug, Display, Formatter};
use std::io::{SeekFrom, Write};
use std::path::Path;
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
use tokio::fs::{File, OpenOptions, read};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use uuid::Uuid;
use crate::logic::messages::{AddFormData, FormMessage, Internal, InternalMessage, ScheduleMessage, ScouterMessage, TemplateMessage};

impl AppState {
    pub fn new(db: Db, config: Settings) -> Self {
        let transaction_path = Path::new(&config.transaction_log_path);

        if !transaction_path.exists() {
            std::fs::File::create(transaction_path).unwrap().write_all("[]".as_bytes()).unwrap();
        }


        Self {
            db_layer: DBLayer::new(db),
            config,
        }
    }

    pub async fn mutate(&self, message: InternalMessage) -> Result<(), Error> {
        match &message.msg {
            Internal::Form(msg) =>
                self.handle_form_message(msg).await?,

            Internal::Template(msg) =>
                self.handle_template_message(msg).await?,

            Internal::Scouter(msg) =>
                self.handle_scouter_message(msg).await?,

            Internal::Schedule(msg) =>
                self.handle_schedule_message(msg).await?
        }

        self.log_mutation(&message).await?;
        self.print_transaction_log().await?;

        Ok(())
    }

    async fn print_transaction_log(&self) -> Result<(), Error> {
        let transaction_log =
            read(&self.config.transaction_log_path).await?;

        let de: Vec<InternalMessage> = serde_json::de::from_slice(&transaction_log)?;

        println!("{:?}", de);

        Ok(())
    }

    async fn log_mutation(&self, message: &InternalMessage) -> Result<(), Error> {
        let mut transaction_log = OpenOptions::default()
            .write(true)
            .read(true)
            .open(&self.config.transaction_log_path).await?;

        let ser = serde_json::to_string(&message)?;

        if transaction_log.seek(SeekFrom::End(-1)).await? > 3 {
            transaction_log.write_all(",\n".as_bytes()).await?;
        }

        transaction_log.write_all(ser.as_bytes()).await?;
        transaction_log.write_all("]".as_bytes()).await?;

        Ok(())
    }

    async fn handle_form_message(&self, message: &FormMessage) -> Result<(), Error> {
        //erm what the freak

        match message {
            FormMessage::Add(data) => self.submit_forms(data).await,
            FormMessage::Remove(data) => self.db_layer.remove_form(&data.template, data.id).await
        }.map_err(|err| err.into())
    }

    async fn handle_template_message(&self, message: &TemplateMessage) -> Result<(), Error> {
        match message {
            TemplateMessage::Add(data) => self.db_layer.add_template(data).await,
            TemplateMessage::Modify(data) => self.db_layer.set_template(data).await,
            TemplateMessage::Remove(data) => self.db_layer.remove_template(data).await
        }.map_err(|err| err.into())
    }

    async fn handle_scouter_message(&self, message: &ScouterMessage) -> Result<(), Error> {
        match message {
            ScouterMessage::Add(_) => { todo!() }
            ScouterMessage::Modify(_) => { todo!() }
            ScouterMessage::Remove(_) => { todo!() }
        }
    }

    async fn handle_schedule_message(&self, message: &ScheduleMessage) -> Result<(), Error> {
        match message {
            ScheduleMessage::Add(data) | ScheduleMessage::Modify(data) =>
                self.db_layer.set_schedule(&data.event, data).await,
            ScheduleMessage::Remove(data) => self.db_layer.remove_schedule(data).await
        }.map_err(|err| err.into())
    }

    pub async fn get_templates(&self) -> Vec<String> {
        self.db_layer.get_templates().await
    }

    pub async fn get_template(&self, template: &str) -> Result<FormTemplate, GetError> {
        self.db_layer.get_template(template).await
    }

    pub async fn build_cache(&self) -> Result<(), GetError> {
        self.db_layer.build_cache().await
    }

    pub async fn get(&self, template: String, filter: Filter) -> Result<Vec<Form>, GetError> {
        self.db_layer.get(template, filter).await
    }

    async fn submit_forms(&self, form: &AddFormData) -> Result<(), SubmitError> {
        self.db_layer.submit_forms(&form.template, &form.forms).await
    }

    pub async fn get_schedule(&self, event: &str) -> Result<Schedule, GetError> {
        self.db_layer.get_schedule(event).await
    }

    pub async fn get_shifts(&self, event: &str, scouter: &str) -> Result<Vec<Shift>, GetError> {
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

impl From<serde_json::Error> for Error {
    fn from(_: serde_json::Error) -> Self {
        Self::Internal
    }
}

impl From<tokio::io::Error> for Error {
    fn from(_: std::io::Error) -> Self {
        Self::Internal
    }
}

impl From<SubmitError> for Error {
    fn from(value: SubmitError) -> Self {
        match value {
            SubmitError::Internal => Self::Internal,
            SubmitError::FormDoesNotFollowTemplate { requested_template } =>
                Self::FormDoesNotFollowTemplate { template: requested_template },
            SubmitError::TemplateDoesNotExist { requested_template } =>
                Self::TemplateDoesNotExist { template: requested_template },
            SubmitError::FormDoesNotExist { id } =>
                Self::UuidDoesNotExist { uuid: id },
            SubmitError::NoScheduleForEvent { event } =>
                Self::ScheduleDoesNotExist { schedule: event }
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
