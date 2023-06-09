use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::fs::read_to_string;
use std::hash::Hash;
use std::path::Path;
use std::sync::Arc;
use actix_web::{HttpResponse, ResponseError};
use actix_web::body::BoxBody;
use actix_web::http::header::ContentType;
use actix_web::http::StatusCode;
use bincode::config::{Configuration};
use bincode::error::EncodeError;
use derive_more::{Display, Error};
use sled::Db;
use tokio::sync::Mutex;
use uuid::Uuid;
use crate::data::Form;
use crate::data::template::FormTemplate;
use crate::state::SubmitError::{FormDoesNotFollowTemplate, Internal, TemplateDoesNotExist};

impl AppState {
    pub fn new(db: Arc<Db>) -> Self {
        let mut templates = HashMap::new();
        let dir = Path::new("templates").read_dir().unwrap();

        for f in dir {
            let ser: FormTemplate = serde_json::from_str(&read_to_string(f.unwrap().path()).unwrap()).unwrap();
        }

        Self {
            db,
            byte_config: bincode::config::standard(),
            templates: Mutex::new(templates),
            cache: Cache::default()
        }
    }

    pub async fn submit_form(&self, template: String, form: &Form) -> Result<(), SubmitError> {
        if let Some(temp) = self.templates.lock().await.get(&template) {
            if !temp.validate_form(form) {
                return Err(FormDoesNotFollowTemplate{ requested_template: template });
            }
        }
        else {
            return Err(TemplateDoesNotExist{ requested_template: template });
        }

        let template_tree = self.db.open_tree(template)?;
        let encoded_form = bincode::encode_to_vec(form, self.byte_config)?;
        let uuid = Uuid::new_v4();

        template_tree.insert(uuid, encoded_form)?;
        self.update_cache(form, uuid).await;

        Ok(())
    }

    pub async fn update_cache(&self, form: &Form, uuid: Uuid) {
        self.cache.add(CacheInput::Team(form.team), uuid).await;
        self.cache.add(CacheInput::Match(form.match_number), uuid).await;
        self.cache.add(CacheInput::Scouter(form.scouter.clone()), uuid).await; //dont like these clones
        self.cache.add(CacheInput::Event(form.event_key.clone()), uuid).await;
    }
}

impl From<EncodeError> for SubmitError {
    fn from(_: EncodeError) -> Self {
        Internal
    }
}

impl From<sled::Error> for SubmitError {
    fn from(_: sled::Error) -> Self {
        Internal
    }
}

impl ResponseError for SubmitError {
    fn status_code(&self) -> StatusCode {
        match self {
            Internal => StatusCode::INTERNAL_SERVER_ERROR,
            FormDoesNotFollowTemplate { .. } => StatusCode::BAD_REQUEST,
            TemplateDoesNotExist { .. } => StatusCode::BAD_REQUEST
        }
    }

    fn error_response(&self) -> HttpResponse<BoxBody> {
        HttpResponse::build(self.status_code())
            .insert_header(ContentType::json())
            .body(self.to_string())
    }
}

impl Cache {
    async fn add(&self, key: CacheInput, val: Uuid) {
        let mut map = self.cache.lock().await;

        match map.get_mut(&key) {
            Some(cache) => { cache.push(val); },
            None => { map.insert(key, vec![val]); }
        };
    }

    async fn get(&self, key: CacheInput) -> Vec<Uuid> {
        let map = self.cache.lock().await;

        match map.get(&key) {
            Some(cache) => { cache.clone() }, //TODO: remove yucky clone
            None => { Vec::new() }
        }
    }
}

#[derive(Default)]
struct Cache {
    cache: Mutex<HashMap<CacheInput, Vec<Uuid>>>
}

#[derive(Eq, Hash, PartialEq)]
enum CacheInput {
    Scouter(String),
    Event(String),
    Match(i64),
    Team(i64)
}

pub struct AppState {
    db: Arc<Db>,
    cache: Cache,
    byte_config: Configuration,
    templates: Mutex<HashMap<String, FormTemplate>>,

}

#[derive(Debug, Display, Error)]
pub enum SubmitError {
    #[display(fmt = "internal error")]
    Internal,

    #[display(fmt = "Form does not follow the template \"{}\"", requested_template)]
    FormDoesNotFollowTemplate { requested_template: String },

    #[display(fmt = "Template \"{}\" does not exist", requested_template)]
    TemplateDoesNotExist { requested_template: String }
}