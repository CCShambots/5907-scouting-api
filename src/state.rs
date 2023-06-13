use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::fs::read_to_string;
use std::hash::Hash;
use std::ops::Deref;
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
use serde::Deserialize;
use sled::Db;
use tokio::sync::Mutex;
use uuid::{Uuid, uuid};
use crate::data::Form;
use crate::data::template::FormTemplate;
use crate::state::CacheInput::{Event, Match, Scouter, Team};
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

    pub async fn get_templates(&self) -> Vec<String> {
        let mut templates: Vec<String> = Vec::new();

        for x in self.templates.lock().await.keys() {
            templates.push(x.clone());
        }

        templates
    }

    pub async fn get_template(&self, template: String) -> Result<FormTemplate, GetError> {
        if self.templates.lock().await.contains_key(&template) {
            let out = self.templates.lock().await.get(&template).cloned().unwrap();

            Ok(out)
        }
        else {
            Err(GetError::TemplateDoesNotExist { template })
        }
    }

    pub async fn build_cache(&self) -> Result<(), GetError> {
        for tree_name in self.db.tree_names() {
            let name = String::from_utf8(tree_name.to_vec()).unwrap();

            if self.templates.lock().await.contains_key(&name) {
                println!("building for tree {}", name);

                let tree = self.db.open_tree(tree_name)?;

                for p in tree.iter() {
                    let pair = p?;
                    let uuid: Uuid = Uuid::from_bytes(uuid::Bytes::try_from(pair.0.to_vec()).unwrap()); //questionable
                    let (form, _): (Form, usize) = bincode::decode_from_slice(&pair.1, self.byte_config)?;

                    self.update_cache(&form, uuid, &name).await?;
                }
            }
        }

        Ok(())
    }

    pub async fn get(&self, template: String, filter: Filter) -> Result<Vec<Form>, GetError> {
        let (cache_result, num_filters) = self.get_uuids_from_filter(&template, &filter).await?;
        let mut map: HashMap<Uuid, i64> = HashMap::new();
        let mut out: Vec<Uuid> = Vec::new();

        for uuid in cache_result {
            if let Some(x) = map.get_mut(&uuid) {
                *x += 1;
            }
            else {
                map.insert(uuid, 1);
            }
        }

        for (uuid, num) in map {
            if num >= num_filters {
                out.push(uuid);
            }
        }

        self.get_forms(&template, &out)
    }

    fn get_forms(&self, template: &String, uuids: &Vec<Uuid>) -> Result<Vec<Form>, GetError> {
        let mut out: Vec<Form> = Vec::new();
        let tree = self.db.open_tree(template)?;

        for x in uuids {
            let (decoded, _): (Form, usize) = bincode::decode_from_slice(&tree.get(x)?.unwrap(), self.byte_config)?;

            out.push(decoded);
        }

        Ok(out)
    }

    async fn get_uuids_from_filter(&self, template: &String, filter: &Filter) -> Result<(Vec<Uuid>, i64), GetError> {
        let mut out: Vec<Uuid> = Vec::new();
        let mut num_filters: i64 = 0;

        if let Some(team) = filter.team {
            out.append(&mut self.cache.get(Team(team), template).await?);
            num_filters += 1;
        }

        if let Some(match_number) = filter.match_number {
            out.append(&mut self.cache.get(Match(match_number), template).await?);
            num_filters += 1;
        }

        if let Some(scouter) = &filter.scouter {
            out.append(&mut self.cache.get(Scouter(scouter.clone()), template).await?);
            num_filters += 1;
        }

        if let Some(event) = &filter.event {
            out.append(&mut self.cache.get(Event(event.clone()), template).await?);
            num_filters += 1;
        }

        if num_filters == 0 {
            return Ok((self.get_all(template).await?, num_filters));
        }

        Ok((out, num_filters))
    }

    pub async fn get_all(&self, template: &String) -> Result<Vec<Uuid>, GetError> {
        let tree = self.db.open_tree(template)?;
        let mut out: Vec<Uuid> = Vec::new();

        for p in tree.iter() {
            let pair = p?;
            let uuid: Uuid = Uuid::from_bytes(uuid::Bytes::try_from(pair.0.to_vec()).unwrap()); //questionable

            out.push(uuid);
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
        self.cache.add(Team(form.team), uuid, template).await?;
        self.cache.add(Match(form.match_number), uuid, template).await?;
        self.cache.add(Scouter(form.scouter.clone()), uuid, template).await?;
        self.cache.add(Event(form.event_key.clone()), uuid, template).await?;

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
        match self {
            GetError::Internal => StatusCode::INTERNAL_SERVER_ERROR,
            GetError::TemplateDoesNotExist { .. } => StatusCode::BAD_REQUEST
        }
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

#[derive(Debug, Deserialize)]
pub struct Filter {
    match_number: Option<i64>,
    team: Option<i64>,
    event: Option<String>,
    scouter: Option<String>
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
    Internal,

    #[display(fmt = "The template \"{}\" does not exist", template)]
    TemplateDoesNotExist { template: String }
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