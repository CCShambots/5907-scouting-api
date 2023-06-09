use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::fs::read_to_string;
use std::hash::Hash;
use std::path::Path;
use std::sync::Arc;
use actix_web::{HttpResponse, ResponseError};
use actix_web::body::{BoxBody, MessageBody};
use actix_web::http::header::ContentType;
use actix_web::http::StatusCode;
use bincode::config::{Configuration};
use bincode::error::{DecodeError, EncodeError};
use derive_more::{Display, Error};
use serde::de::Unexpected::Map;
use sled::Db;
use tokio::sync::Mutex;
use uuid::{Uuid, uuid};
use crate::data::Form;
use crate::data::template::FormTemplate;
use crate::state::SubmitError::{FormDoesNotFollowTemplate, Internal, TemplateDoesNotExist};

impl AppState {
    pub fn new(db: Db) -> Self {
        let mut templates = HashMap::new();
        let mut template_names: Vec<String> = Vec::new();
        let dir = Path::new("templates").read_dir().unwrap();

        for f in dir {
            let ser: FormTemplate = serde_json::from_str(&read_to_string(f.unwrap().path()).unwrap()).unwrap();
            template_names.push(ser.name.clone());
            templates.insert(ser.name.clone(), ser);
        }

        Self {
            db,
            byte_config: bincode::config::standard(),
            templates: Mutex::new(templates),
            cache: Cache::new(template_names)
        }
    }

    pub async fn build_cache(&self) -> Result<(), GetError> {
        for tree_name in self.db.tree_names() {
            let name = String::from_utf8(tree_name.to_vec()).unwrap();

            println!("building for tree {}", name);

            let tree = self.db.open_tree(tree_name)?;

            for p in tree.iter() {
                let pair = p?;
                let uuid: Uuid = Uuid::from_bytes(uuid::Bytes::try_from(pair.0.to_vec()).unwrap()); //questionable
                let (form, _): (Form, usize) = bincode::decode_from_slice(&pair.1, self.byte_config)?;

                self.update_cache(&form, uuid, &name).await?;
            }
        }

        Ok(())
    }

    pub async fn get_all_for_team(&self, template: String, team: i64) -> Result<Vec<Form>, GetError> {
        let mut out: Vec<Form> = Vec::new();
        let tree = self.db.open_tree(template.clone())?;

        for x in self.cache.get(CacheInput::Team(team), &template).await? {
            println!("{} found in cache", x);
            let (decoded, _): (Form, usize) = bincode::decode_from_slice(&tree.get(x)?.unwrap(), self.byte_config)?;

            out.push(decoded);
        }

        Ok(out)
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

        let template_tree = self.db.open_tree(template.clone())?;
        let encoded_form = bincode::encode_to_vec(form, self.byte_config)?;
        let uuid = Uuid::new_v4();

        template_tree.insert(uuid, encoded_form)?;
        self.update_cache(form, uuid, &template).await?;

        Ok(())
    }

    pub async fn update_cache(&self, form: &Form, uuid: Uuid, template: &String) -> Result<(), GetError> {
        self.cache.add(CacheInput::Team(form.team), uuid, template).await?;
        self.cache.add(CacheInput::Match(form.match_number), uuid, template).await?;
        self.cache.add(CacheInput::Scouter(form.scouter.clone()), uuid, template).await?;
        self.cache.add(CacheInput::Event(form.event_key.clone()), uuid, template).await?;

        Ok(())
    }
}

impl From<uuid::Error> for GetError {
    fn from(_: uuid::Error) -> Self {
        GetError::Internal
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

impl From<DecodeError> for GetError {
    fn from(_: DecodeError) -> Self {
        GetError::Internal
    }
}

impl From<sled::Error> for GetError {
    fn from(_: sled::Error) -> Self {
        GetError::Internal
    }
}

impl From<serde_json::Error> for GetError {
    fn from(_: serde_json::Error) -> Self {
        GetError::Internal
    }
}

impl From<GetError> for SubmitError {
    fn from(_: GetError) -> Self {
        Internal
    }
}

impl ResponseError for GetError {
    fn status_code(&self) -> StatusCode {
        StatusCode::INTERNAL_SERVER_ERROR
    }

    fn error_response(&self) -> HttpResponse<BoxBody> {
        HttpResponse::build(self.status_code())
            .insert_header(ContentType::json())
            .body(self.to_string())
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
    fn new(templates: Vec<String>) -> Self {
        let mut out = Self {
            cache: HashMap::new()
        };

        for temp in templates {
            out.cache.insert(temp, Mutex::new(HashMap::new()));
        }

        out
    }

    async fn add(&self, key: CacheInput, val: Uuid, template: &String) -> Result<(), GetError> {
        let mut map = self.try_get_template_cache(template)?.lock().await;

        match map.get_mut(&key) {
            Some(cache) => { cache.push(val); },
            None => { map.insert(key, vec![val]); }
        };

        println!("Cache updated");

        Ok(())
    }

    async fn clear(&mut self) {
        for cache in &mut self.cache {
            cache.1.lock().await.clear();
        }
    }

    fn try_get_template_cache(&self, template: &String) -> Result<&Mutex<HashMap<CacheInput, Vec<Uuid>>>, GetError> {
        match self.cache.get(template) {
            None => { Err(GetError::Internal) },
            Some(map) => { Ok(map) }
        }
    }

    async fn get(&self, key: CacheInput, template: &String) -> Result<Vec<Uuid>, GetError> {
        match self.try_get_template_cache(template)?.lock().await.get(&key) {
            Some(cache) => { Ok(cache.clone()) }, //TODO: remove yucky clone
            None => { Ok(Vec::new()) }
        }
    }
}

#[derive(Default)]
struct Cache {
    cache: HashMap<String, Mutex<HashMap<CacheInput, Vec<Uuid>>>>
}

#[derive(Eq, Hash, PartialEq)]
enum CacheInput {
    Scouter(String),
    Event(String),
    Match(i64),
    Team(i64)
}

pub struct AppState {
    db: Db,
    cache: Cache,
    byte_config: Configuration,
    templates: Mutex<HashMap<String, FormTemplate>>,

}

#[derive(Debug, Display, Error)]
pub enum GetError {
    #[display(fmt = "Internal error")]
    Internal
}

#[derive(Debug, Display, Error)]
pub enum SubmitError {
    #[display(fmt = "Internal error")]
    Internal,

    #[display(fmt = "Form does not follow the template \"{}\"", requested_template)]
    FormDoesNotFollowTemplate { requested_template: String },

    #[display(fmt = "Template \"{}\" does not exist", requested_template)]
    TemplateDoesNotExist { requested_template: String }
}