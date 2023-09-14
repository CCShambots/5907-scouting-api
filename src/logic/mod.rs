pub mod messages;

use std::fmt::{Debug, Display, Formatter};
use std::io::{SeekFrom, Write};
use std::path::Path;
use actix_web::http::StatusCode;
use actix_web::{HttpResponse, ResponseError};
use actix_web::body::BoxBody;
use actix_web::http::header::ContentType;
use derive_more::{Display, Error};
use serde::Serialize;
use crate::data::db_layer::{DBLayer, Filter, Error as DBError, ItemType};
use crate::data::template::FormTemplate;
use crate::data::{Form, Schedule, Scouter, Shift};
use crate::settings::Settings;
use sled::Db;
use tokio::fs::{File, OpenOptions, read};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use uuid::Uuid;
use crate::logic::messages::{Internal, InternalMessage};

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

    pub async fn mutate(&self, message: InternalMessage) -> Result<String, Error> {
        let key = match &message.msg {
            Internal::Add(msg) => self.db_layer.add(msg).await,
            Internal::Remove(msg) => self.db_layer.remove(msg).await,
            Internal::Edit(msg) => self.db_layer.edit(msg).await
        }?;

        self.log_mutation(&message).await?;
        println!("{:?}", &message);

        Ok(key)
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

    pub async fn get_bytes_by_key(&self, key: &str) -> Result<Vec<u8>, Error> {
        self.db_layer.get_bytes_by_key(key).await.map_err(|x| x.into())
    }

    pub async fn get_bytes(&self) -> Result<Vec<String>, Error> {
        self.db_layer.get_bytes().await.map_err(|x| x.into())
    }

    pub async fn get_schedule_events(&self) -> Result<Vec<String>, Error> {
        self.db_layer.get_schedule_events().await.map_err(|x| x.into())
    }

    pub async fn get_scouter(&self, key: &str) -> Result<Scouter, Error> {
        self.db_layer.get_scouter(key).await.map_err(|x| x.into())
    }

    pub async fn get_scouters(&self) -> Result<Vec<String>, Error> {
        self.db_layer.get_scouters().await.map_err(|x| x.into())
    }

    pub async fn get_templates(&self) -> Vec<String> {
        self.db_layer.get_templates().await
    }

    pub async fn get_template(&self, template: &str) -> Result<FormTemplate, Error> {
        self.db_layer.get_template(template).await.map_err(|err| err.into())
    }

    pub async fn build_cache(&self) -> Result<(), Error> {
        self.db_layer.build_cache().await.map_err(|err| err.into())
    }

    pub async fn get(&self, template: String, filter: Filter) -> Result<Vec<(Form, Uuid)>, Error> {
        self.db_layer.get(template, filter).await.map_err(|err| err.into())
    }

    pub async fn get_form_from_id(&self, template: &str, id: Uuid) -> Result<(Form, Uuid), Error> {
        Ok(self.db_layer.get_form(template, id).await?)
    }

    pub async fn get_schedule(&self, event: &str) -> Result<Schedule, Error> {
        self.db_layer.get_schedule(event).await.map_err(|err| err.into())
    }

    pub async fn get_shifts(&self, event: &str, scouter: &str) -> Result<Vec<Shift>, Error> {
        self.db_layer.get_shifts(event, scouter).await.map_err(|err| err.into())
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

impl From<DBError> for Error {
    fn from(value: DBError) -> Self {
        match value {
            DBError::DoesNotExist(err) => Self::DoesNotExist { item_type: err },
            DBError::ExistsAlready(err) => Self::ExistsAlready { item_type: err },
            DBError::FormDoesNotFollowTemplate { template } => Error::FormDoesNotFollowTemplate { template },
            DBError::TemplateHasForms { template } => Error::TemplateHasForms { template },
            _ => Error::Internal
        }
    }
}

#[derive(Debug, Display)]
pub enum Error {
    Internal,

    #[display(fmt = "{item_type} Does Not Exist")]
    DoesNotExist{ item_type: ItemType },

    #[display(fmt = "{item_type} Exists Already")]
    ExistsAlready{ item_type: ItemType },

    #[display(fmt = "{name} is a reserved word")]
    TemplateNameReserved { name: String },

    #[display(fmt = "The form submitted does not follow the template: {template}")]
    FormDoesNotFollowTemplate { template: String },

    #[display(fmt = "The template {template} is immutable because it has forms saved")]
    TemplateHasForms { template: String }
}
