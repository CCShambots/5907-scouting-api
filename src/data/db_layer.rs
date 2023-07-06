use crate::data::db_layer::CacheInput::{Event, Match, Scouter, Team};
use crate::data::db_layer::SubmitError::{FormDoesNotExist, FormDoesNotFollowTemplate, TemplateDoesNotExist};
use crate::data::template::{Error, FormTemplate};
use crate::data::{Form, Schedule, Shift};
use actix_web::body::BoxBody;
use actix_web::http::header::ContentType;
use actix_web::http::StatusCode;
use actix_web::{HttpResponse, ResponseError};
use bincode::config::{Configuration};
use bincode::error::{DecodeError, EncodeError};
use derive_more::{Display, Error};
use serde::Deserialize;
use sled::{Batch, Db, Transactional};
use std::array::TryFromSliceError;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::fs::read_to_string;
use std::io::Read;
use std::ops::BitAnd;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use uuid::{Uuid};


impl DBLayer {
    pub fn new(db: Db) -> Self {
        Self {
            db,
            byte_config: bincode::config::standard(),
            cache: RwLock::new(Cache::new()),
            //avoid buffoonery and or shenanigans
            reserved_trees: HashSet::from([
                "__sled__default".into(),
                "schedules".into(),
                "scouters".into(),
                "templates".into()
            ])
        }
    }

    pub async fn get(&self, template: String, filter: Filter) -> Result<Vec<Form>, GetError> {
        let cache_result = self.get_uuids_from_filter(&template, &filter).await?;
        let mut set = cache_result[0].clone();

        for x in &cache_result[1..] {
            set = set.bitand(x);
        }

        self.get_forms(&template, set.iter().collect())
    }

    fn get_forms(&self, template: &String, uuids: Vec<&Uuid>) -> Result<Vec<Form>, GetError> {
        let tree = self.db.open_tree(template)?;

        Ok(
            uuids
                .iter()
                .map(|x| bincode::decode_from_slice(&tree.get(x).unwrap().unwrap(), self.byte_config).unwrap().0)
                .collect()
        )
    }

    pub async fn get_schedule(&self, event: &str) -> Result<Schedule, GetError> {
        let tree = self.db.open_tree("schedules")?;

        if !tree.contains_key(event)? {
            return Err(GetError::NoScheduleForEvent { event: event.into() });
        }

        let (decoded, _): (Schedule, usize) =
            bincode::decode_from_slice(&tree.get(event)?.unwrap(), self.byte_config)?;

        Ok(decoded)
    }

    pub async fn set_schedule(&self, event: &str, schedule: &Schedule) -> Result<(), SubmitError> {
        let tree = self.db.open_tree("schedules")?;

        tree.insert(event, bincode::encode_to_vec(schedule, self.byte_config)?)?;

        Ok(())
    }

    pub async fn remove_schedule(&self, event: &str) -> Result<(), SubmitError> {
        let tree = self.db.open_tree("schedules")?;

        match tree.contains_key(event)? {
            true => {
                tree.remove(event)?;
                Ok(())
            },
            false => Err(SubmitError::NoScheduleForEvent { event: event.into() })
        }
    }

    pub async fn get_shifts(&self, event: &str, scouter: &str) -> Result<Vec<Shift>, GetError> {
        let schedule = self.get_schedule(event).await?;

        Ok(schedule
            .shifts
            .into_iter()
            .filter(|shift| shift.scouter == scouter)
            .collect()
        )
    }

    pub async fn submit_forms(&self, template: &str, forms: &[Form]) -> Result<(), SubmitError> {
        let mut op = Batch::default();
        let template_tree = self.db.open_tree(template)?;
        let temp = self.get_template(template).await
            .map_err(|_| TemplateDoesNotExist { requested_template: template.into() })?;


        for form in forms.to_vec().iter_mut() {
            temp.validate_form(form)?;

            let uuid = Uuid::try_parse(form.id.get_or_insert(Uuid::new_v4().to_string()))?;

            let encoded_form = bincode::encode_to_vec(&*form, self.byte_config)?;

            op.insert(uuid.as_bytes(), encoded_form);

            self.update_cache(form, uuid, template).await?;
        }

        template_tree.apply_batch(op)?;

        Ok(())
    }

    pub async fn remove_form(&self, template: &str, id: Uuid) -> Result<(), SubmitError> {
        let tree = self.db.open_tree(template)?;

        match tree.contains_key(id)? {
            true => {
                tree.remove(id)?;
                Ok(())
            },
            false => Err(FormDoesNotExist { id })
        }
    }

    pub async fn add_template(&self, template: &FormTemplate) -> Result<(), SubmitError> {
        let tree = self.db.open_tree("templates")?;

        match !tree.contains_key(&template.name)? {
            true => {
                self.cache.write().await.add_template(&template.name)?;
                self.set_template(template).await
            },
            false => Ok(())
        }
    }

    pub async fn set_template(&self, template: &FormTemplate) -> Result<(), SubmitError> {
        let tree = self.db.open_tree("templates")?;

        tree.insert(&template.name, bincode::encode_to_vec(template, self.byte_config)?)?;

        Ok(())
    }

    pub async fn remove_template(&self, name: &str) -> Result<(), SubmitError> {
        let tree = self.db.open_tree("templates")?;

        match tree.contains_key(name)? {
            true => {
                tree.remove(name)?;
                self.cache.write().await.remove_template(name)
            },
            false => Err(TemplateDoesNotExist { requested_template: name.into() })
        }
    }

    pub async fn get_templates(&self) -> Vec<String> {
        self.db.open_tree("templates")
            .unwrap()
            .iter()
            .keys()
            .map(|k| String::from_utf8(k.unwrap().to_vec()).unwrap())
            .collect()
    }

    pub async fn get_template(&self, template: &str) -> Result<FormTemplate, GetError> {
        match self.db.open_tree("templates")?.get(template)? {
            Some(temp) => Ok(bincode::decode_from_slice(&temp, self.byte_config)?.0),
            None => Err(GetError::TemplateDoesNotExist { template: template.into() }),
        }
    }

    //get all form ids that follow a template
    async fn get_all(&self, template: &String) -> Result<HashSet<Uuid>, GetError> {
        let tree = self.db.open_tree(template)?;

        //im cray cray
        Ok(HashSet::from_iter(tree
            .iter()
            .keys()
            .map(|bytes| Uuid::from_slice(&bytes.unwrap()).unwrap()))
        )
    }

    async fn get_uuids_from_filter(
        &self,
        template: &String,
        filter: &Filter,
    ) -> Result<Vec<HashSet<Uuid>>, GetError> {
        let mut out: Vec<HashSet<Uuid>> = Vec::new();
        let mut num_filters: i64 = 0;
        let lock = self.cache.read().await;

        if let Some(team) = filter.team {
            out.push(lock.get(Team(team), template)?);
            num_filters += 1;
        }

        if let Some(match_number) = filter.match_number {
            out.push(lock.get(Match(match_number), template)?);
            num_filters += 1;
        }

        if let Some(scouter) = &filter.scouter {
            out.push(lock.get(Scouter(scouter.clone()), template)?);
            num_filters += 1;
        }

        if let Some(event) = &filter.event {
            out.push(lock.get(Event(event.clone()), template)?);
            num_filters += 1;
        }

        if num_filters == 0 {
            return Ok(vec![self.get_all(template).await?]);
        }

        Ok(out)
    }

    fn template_exists(&self, template: &str) -> bool {
        self.db.open_tree("templates").unwrap().contains_key(template).unwrap()
    }

    pub async fn build_cache(&self) -> Result<(), GetError> {
        for tree_name in self.db.tree_names() {
            let name = String::from_utf8(tree_name.to_vec()).unwrap();

            if self.template_exists(&name) {
                println!("building for tree {}", name);
                self.cache.write().await.add_template(&name).unwrap();

                let tree = self.db.open_tree(tree_name)?;

                for p in tree.iter() {
                    let pair = p?;
                    let uuid: Uuid = Uuid::from_slice(&pair.0)?;
                    let (form, _): (Form, usize) =
                        bincode::decode_from_slice(&pair.1, self.byte_config)?;

                    self.update_cache(&form, uuid, &name).await?;
                }
            }
        }

        Ok(())
    }

    pub async fn update_cache(
        &self,
        form: &Form,
        uuid: Uuid,
        template: &str,
    ) -> Result<(), GetError> {
        let mut lock = self.cache.write().await;

        lock.add(Team(form.team), uuid, template)?;
        lock.add(Match(form.match_number), uuid, template)?;
        lock.add(Scouter(form.scouter.clone()), uuid, template)?;
        lock.add(Event(form.event_key.clone()), uuid, template)?;

        Ok(())
    }
}

impl Cache {
    fn new() -> Self {
        Self {
            cache: HashMap::new()
        }
    }

    fn add_template(&mut self, template: &str) -> Result<(), SubmitError> {
        if !self.cache.contains_key(template) {
            self.cache.insert(template.to_owned(), HashMap::new());
        }

        Ok(())
    }

    fn remove_template(&mut self, template: &str) -> Result<(), SubmitError> {
        self.cache.remove(template);

        Ok(())
    }

    fn add(&mut self, key: CacheInput, val: Uuid, template: &str) -> Result<(), GetError> {
        let map = self.cache
            .get_mut(template)
            .ok_or(GetError::TemplateDoesNotExist { template: template.into() })?;

        match map.get_mut(&key) {
            Some(cache) => {
                cache.insert(val);
            }
            None => {
                map.insert(key, HashSet::from([val]));
            }
        };

        Ok(())
    }

    fn clear(&mut self) {
        for cache in self.cache.iter_mut() {
            cache.1.clear();
        }
    }

    fn get(&self, key: CacheInput, template: &str) -> Result<HashSet<Uuid>, GetError> {
        match self.cache
            .get(template)
            .ok_or(GetError::TemplateDoesNotExist { template: template.into() })?
            .get(&key)
        {
            Some(cache) => Ok(cache.clone()),
            None => Ok(HashSet::new()),
        }
    }
}

impl From<TryFromSliceError> for GetError {
    fn from(_: TryFromSliceError) -> Self {
        GetError::Internal
    }
}

impl From<uuid::Error> for GetError {
    fn from(_: uuid::Error) -> Self {
        GetError::Internal
    }
}

impl From<EncodeError> for SubmitError {
    fn from(_: EncodeError) -> Self {
        SubmitError::Internal
    }
}

impl From<sled::Error> for SubmitError {
    fn from(_: sled::Error) -> Self {
        SubmitError::Internal
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
        SubmitError::Internal
    }
}

impl From<crate::data::template::Error> for SubmitError {
    fn from(v: Error) -> Self {
        TemplateDoesNotExist { requested_template: v.template }
    }
}

impl From<uuid::Error> for SubmitError {
    fn from(_: uuid::Error) -> Self {
        Self::Internal
    }
}

impl ResponseError for GetError {
    fn status_code(&self) -> StatusCode {
        match self {
            GetError::Internal => StatusCode::INTERNAL_SERVER_ERROR,
            GetError::TemplateDoesNotExist { .. } => StatusCode::BAD_REQUEST,
            GetError::NoScheduleForEvent { .. } => StatusCode::BAD_REQUEST,
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
            SubmitError::Internal => StatusCode::INTERNAL_SERVER_ERROR,
            FormDoesNotFollowTemplate { .. } => StatusCode::BAD_REQUEST,
            TemplateDoesNotExist { .. } => StatusCode::BAD_REQUEST,
            FormDoesNotExist { .. } => StatusCode::BAD_REQUEST,
            SubmitError::NoScheduleForEvent { .. } => StatusCode::BAD_REQUEST
        }
    }

    fn error_response(&self) -> HttpResponse<BoxBody> {
        HttpResponse::build(self.status_code())
            .insert_header(ContentType::json())
            .body(self.to_string())
    }
}

#[derive(Debug, Display, Error)]
pub enum GetError {
    #[display(fmt = "Internal error")]
    Internal,

    #[display(fmt = "The template \"{}\" does not exist", template)]
    TemplateDoesNotExist { template: String },

    #[display(fmt = "There is not a schedule for event \"{event}\"")]
    NoScheduleForEvent { event: String },
}

#[derive(Debug, Display, Error)]
pub enum SubmitError {
    #[display(fmt = "Internal error")]
    Internal,

    #[display(fmt = "Form does not follow the template \"{}\"", requested_template)]
    FormDoesNotFollowTemplate { requested_template: String },

    #[display(fmt = "Template \"{}\" does not exist", requested_template)]
    TemplateDoesNotExist { requested_template: String },

    FormDoesNotExist { id: Uuid },

    NoScheduleForEvent { event: String }
}

pub struct DBLayer {
    db: Db,
    cache: RwLock<Cache>,
    byte_config: Configuration,
    reserved_trees: HashSet<String>
}

#[derive(Default)]
struct Cache {
    cache: HashMap<String, HashMap<CacheInput, HashSet<Uuid>>>,
}

#[derive(Eq, Hash, PartialEq)]
enum CacheInput {
    Scouter(String),
    Event(String),
    Match(i64),
    Team(i64),
}

#[derive(Debug, Deserialize)]
pub struct Filter {
    match_number: Option<i64>,
    team: Option<i64>,
    event: Option<String>,
    scouter: Option<String>,
}
