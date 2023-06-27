use std::array::TryFromSliceError;
use std::collections::{HashMap, HashSet};
use bincode::config::{BigEndian, Configuration};
use sled::{Db, IVec};
use uuid::{Uuid, uuid};
use serde::Deserialize;
use crate::data::template::FormTemplate;
use tokio::sync::Mutex;
use std::fmt::Debug;
use std::fs::read_to_string;
use std::ops::BitAnd;
use std::path::Path;
use actix_web::http::StatusCode;
use actix_web::{HttpResponse, ResponseError};
use actix_web::body::BoxBody;
use actix_web::http::header::ContentType;
use bincode::error::{DecodeError, EncodeError};
use derive_more::{Display, Error};
use crate::data::db_layer::CacheInput::{Event, Match, Scouter, Team};
use crate::data::{Form, Schedule, Shift};
use crate::data::db_layer::SubmitError::{FormDoesNotFollowTemplate, TemplateDoesNotExist};

impl DBLayer {
    pub fn new(db: Db, template_path: &str) -> Self {
        let mut templates = HashMap::new();
        let mut template_names: Vec<String> = Vec::new();
        let dir = Path::new(template_path).read_dir().unwrap();

        for f in dir {
            let ser: FormTemplate =
                serde_json::from_str(&read_to_string(f.unwrap().path()).unwrap()).unwrap();

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

    pub async fn get(&self, template: String, filter: Filter) -> Result<Vec<Form>, GetError> {
        let cache_result = self.get_uuids_from_filter(&template, &filter).await?;
        let mut set: HashSet<&Uuid> = HashSet::from_iter(cache_result[0].iter());

        for x in &cache_result[1..] {
            set = set.bitand(&HashSet::from_iter(x.iter()));
        }

        self.get_forms(&template, set.iter().copied().collect())
    }

    fn get_forms(&self, template: &String, uuids: Vec<&Uuid>) -> Result<Vec<Form>, GetError> {
        let mut out: Vec<Form> = Vec::new();
        let tree = self.db.open_tree(template)?;

        for x in uuids {
            let (decoded, _): (Form, usize) =
                bincode::decode_from_slice(&tree.get(x)?.unwrap(), self.byte_config)?;

            out.push(decoded);
        }

        Ok(out)
    }

    pub async fn get_schedule(&self, event: String) -> Result<Schedule, GetError> {
        let tree = self.db.open_tree("schedules")?;

        if !tree.contains_key(&event)? {
            return Err(GetError::NoScheduleForEvent { event });
        }

        let (decoded, _): (Schedule, usize) =
            bincode::decode_from_slice(&tree.get(event)?.unwrap(), self.byte_config)?;

        Ok(decoded)
    }

    pub async fn set_schedule(&self, event: String, schedule: Schedule) -> Result<(), SubmitError> {
        let tree = self.db.open_tree("schedules")?;

        tree.insert(event, bincode::encode_to_vec(schedule, self.byte_config)?)?;

        Ok(())
    }

    pub async fn get_shifts(&self, event: String, scouter: String) -> Result<Vec<Shift>, GetError> {
        let schedule = self.get_schedule(event).await?;

        Ok(schedule.shifts
            .iter()
            .filter(|shift| shift.scouter == scouter)
            .cloned()
            .collect()
        )
    }

    pub async fn submit_form(&self, template: String, form: &Form) -> Result<(), SubmitError> {
        if let Some(temp) = self.templates.lock().await.get(&template) {
            if !temp.validate_form(form) {
                return Err(FormDoesNotFollowTemplate {
                    requested_template: template,
                });
            }
        } else {
            return Err(TemplateDoesNotExist {
                requested_template: template,
            });
        }

        let template_tree = self.db.open_tree(template.clone())?;
        let encoded_form = bincode::encode_to_vec(form, self.byte_config)?;
        let uuid = Uuid::new_v4();

        template_tree.insert(uuid, encoded_form)?;
        self.update_cache(form, uuid, &template).await?;

        Ok(())
    }

    pub async fn get_templates(&self) -> Vec<String> {
        self.templates.lock().await.keys().cloned().collect()
    }

    pub async fn get_template(&self, template: String) -> Result<FormTemplate, GetError> {
        match self.templates.lock().await.get(&template) {
            Some(temp) => Ok(temp.clone()),
            None => Err(GetError::TemplateDoesNotExist { template })
        }
    }

    //get all form ids that follow a template
    async fn get_all(&self, template: &String) -> Result<Vec<Uuid>, GetError> {
        let tree = self.db.open_tree(template)?;

        //im cray cray
        Ok(
            tree.iter()
                .keys()
                .map(|bytes| Uuid::from_slice(&bytes.unwrap()).unwrap())
                .collect()
        )
    }

    async fn get_uuids_from_filter(
        &self,
        template: &String,
        filter: &Filter,
    ) -> Result<Vec<Vec<Uuid>>, GetError> {
        let mut out: Vec<Vec<Uuid>> = Vec::new();
        let mut num_filters: i64 = 0;

        if let Some(team) = filter.team {
            out.push(self.cache.get(Team(team), template).await?);
            num_filters += 1;
        }

        if let Some(match_number) = filter.match_number {
            out.push(self.cache.get(Match(match_number), template).await?);
            num_filters += 1;
        }

        if let Some(scouter) = &filter.scouter {
            out.push(self.cache.get(Scouter(scouter.clone()), template).await?);
            num_filters += 1;
        }

        if let Some(event) = &filter.event {
            out.push(self.cache.get(Event(event.clone()), template).await?);
            num_filters += 1;
        }

        if num_filters == 0 {
            return Ok(vec![self.get_all(template).await?]);
        }

        Ok(out)
    }

    pub async fn build_cache(&self) -> Result<(), GetError> {
        for tree_name in self.db.tree_names() {
            let name = String::from_utf8(tree_name.to_vec()).unwrap();

            if self.templates.lock().await.contains_key(&name) {
                println!("building for tree {}", name);

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
        template: &String,
    ) -> Result<(), GetError> {
        self.cache.add(Team(form.team), uuid, template).await?;
        self.cache
            .add(Match(form.match_number), uuid, template)
            .await?;
        self.cache
            .add(Scouter(form.scouter.clone()), uuid, template)
            .await?;
        self.cache
            .add(Event(form.event_key.clone()), uuid, template)
            .await?;

        Ok(())
    }
}

impl Cache {
    fn new(templates: Vec<String>) -> Self {
        let mut out = Self {
            cache: HashMap::new(),
        };

        for temp in templates {
            out.cache.insert(temp, Mutex::new(HashMap::new()));
        }

        out
    }

    async fn add(&self, key: CacheInput, val: Uuid, template: &String) -> Result<(), GetError> {
        let mut map = self.try_get_template_cache(template)?.lock().await;

        match map.get_mut(&key) {
            Some(cache) => {
                cache.push(val);
            }
            None => {
                map.insert(key, vec![val]);
            }
        };

        Ok(())
    }


    async fn clear(&mut self) {
        for cache in &mut self.cache {
            cache.1.lock().await.clear();
        }
    }

    fn try_get_template_cache(
        &self,
        template: &String,
    ) -> Result<&Mutex<HashMap<CacheInput, Vec<Uuid>>>, GetError> {
        match self.cache.get(template) {
            None => Err(GetError::Internal),
            Some(map) => Ok(map),
        }
    }

    async fn get(&self, key: CacheInput, template: &String) -> Result<Vec<Uuid>, GetError> {
        match self
            .try_get_template_cache(template)?
            .lock()
            .await
            .get(&key)
        {
            Some(cache) => Ok(cache.clone()), //TODO: remove yucky clone
            None => Ok(Vec::new()),
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

impl ResponseError for GetError {
    fn status_code(&self) -> StatusCode {
        match self {
            GetError::Internal => StatusCode::INTERNAL_SERVER_ERROR,
            GetError::TemplateDoesNotExist { .. } => StatusCode::BAD_REQUEST,
            GetError::NoScheduleForEvent { .. } => StatusCode::BAD_REQUEST
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
            SubmitError::FormDoesNotFollowTemplate { .. } => StatusCode::BAD_REQUEST,
            SubmitError::TemplateDoesNotExist { .. } => StatusCode::BAD_REQUEST,
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
    NoScheduleForEvent { event: String }
}

#[derive(Debug, Display, Error)]
pub enum SubmitError {
    #[display(fmt = "Internal error")]
    Internal,

    #[display(fmt = "Form does not follow the template \"{}\"", requested_template)]
    FormDoesNotFollowTemplate { requested_template: String },

    #[display(fmt = "Template \"{}\" does not exist", requested_template)]
    TemplateDoesNotExist { requested_template: String },
}

pub struct DBLayer {
    db: Db,
    cache: Cache,
    byte_config: Configuration,
    templates: Mutex<HashMap<String, FormTemplate>>
}

#[derive(Default)]
struct Cache {
    cache: HashMap<String, Mutex<HashMap<CacheInput, Vec<Uuid>>>>,
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