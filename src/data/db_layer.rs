use crate::data::db_layer::CacheInput::{Event, Match, Scouter, Team};
use crate::data::template::{FormTemplate};
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
use uuid::{Error as UuidError, Uuid};
use crate::data::template::Error as TemplateError;


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

    pub async fn get(&self, template: String, filter: Filter) -> Result<Vec<Form>, Error> {
        let cache_result = self.get_uuids_from_filter(&template, &filter).await?;
        let mut set = cache_result[0].clone();

        for x in &cache_result[1..] {
            set = set.bitand(x);
        }

        self.get_forms(&template, set.iter().collect())
    }

    fn get_forms(&self, template: &String, uuids: Vec<&Uuid>) -> Result<Vec<Form>, Error> {
        let tree = self.db.open_tree(template)?;

        Ok(
            uuids
                .iter()
                .map(|x| bincode::decode_from_slice(&tree.get(x).unwrap().unwrap(), self.byte_config).unwrap().0)
                .collect()
        )
    }

    pub async fn get_schedule(&self, event: &str) -> Result<Schedule, Error> {
        let tree = self.db.open_tree("schedules")?;

        if !tree.contains_key(event)? {
            return Err(Error::DoesNotExist(ItemType::Schedule(event.into())));
        }

        let (decoded, _): (Schedule, usize) =
            bincode::decode_from_slice(&tree.get(event)?.unwrap(), self.byte_config)?;

        Ok(decoded)
    }

    pub async fn set_schedule(&self, event: &str, schedule: &Schedule) -> Result<(), Error> {
        let tree = self.db.open_tree("schedules")?;

        tree.insert(event, bincode::encode_to_vec(schedule, self.byte_config)?)?;

        Ok(())
    }

    pub async fn remove_schedule(&self, event: &str) -> Result<(), Error> {
        let tree = self.db.open_tree("schedules")?;

        match tree.contains_key(event)? {
            true => {
                tree.remove(event)?;
                Ok(())
            },
            false => Err(Error::DoesNotExist(ItemType::Schedule(event.into())))
        }
    }

    pub async fn get_shifts(&self, event: &str, scouter: &str) -> Result<Vec<Shift>, Error> {
        let schedule = self.get_schedule(event).await?;

        Ok(schedule
            .shifts
            .into_iter()
            .filter(|shift| shift.scouter == scouter)
            .collect()
        )
    }

    pub async fn submit_forms(&self, template: &str, forms: &[Form]) -> Result<(), Error> {
        let mut op = Batch::default();
        let template_tree = self.db.open_tree(template)?;
        let temp = self.get_template(template).await
            .map_err(|_| Error::DoesNotExist(ItemType::Template(template.into())))?;


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

    pub async fn remove_form(&self, template: &str, id: Uuid) -> Result<(), Error> {
        let tree = self.db.open_tree(template)?;

        match tree.contains_key(id)? {
            true => {
                tree.remove(id)?;
                Ok(())
            },
            false => Err(Error::DoesNotExist(ItemType::Form(id.to_string())))
        }
    }

    pub async fn add_template(&self, template: &FormTemplate) -> Result<(), Error> {
        let tree = self.db.open_tree("templates")?;

        match !tree.contains_key(&template.name)? {
            true => {
                self.cache.write().await.add_template(&template.name)?;
                self.set_template(template).await
            },
            false => Ok(())
        }
    }

    pub async fn set_template(&self, template: &FormTemplate) -> Result<(), Error> {
        let tree = self.db.open_tree("templates")?;

        tree.insert(&template.name, bincode::encode_to_vec(template, self.byte_config)?)?;

        Ok(())
    }

    pub async fn remove_template(&self, name: &str) -> Result<(), Error> {
        let tree = self.db.open_tree("templates")?;

        match tree.contains_key(name)? {
            true => {
                tree.remove(name)?;
                self.cache.write().await.remove_template(name)
            },
            false => Err(Error::DoesNotExist(ItemType::Template(name.into())))
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

    pub async fn get_template(&self, template: &str) -> Result<FormTemplate, Error> {
        match self.db.open_tree("templates")?.get(template)? {
            Some(temp) => Ok(bincode::decode_from_slice(&temp, self.byte_config)?.0),
            None => Err(Error::DoesNotExist(ItemType::Template(template.into()))),
        }
    }

    //get all form ids that follow a template
    async fn get_all(&self, template: &String) -> Result<HashSet<Uuid>, Error> {
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
    ) -> Result<Vec<HashSet<Uuid>>, Error> {
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

    pub async fn build_cache(&self) -> Result<(), Error> {
        for tree_name in self.db.open_tree("templates")?.iter().keys() {
            let unwrapped = tree_name?;
            let name = String::from_utf8(unwrapped.to_vec()).unwrap();

            if self.template_exists(&name) {
                println!("building for tree {}", name);
                self.cache.write().await.add_template(&name).unwrap();

                let tree = self.db.open_tree(unwrapped)?;

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
    ) -> Result<(), Error> {
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

    fn add_template(&mut self, template: &str) -> Result<(), Error> {
        if !self.cache.contains_key(template) {
            self.cache.insert(template.to_owned(), HashMap::new());
        }

        Ok(())
    }

    fn remove_template(&mut self, template: &str) -> Result<(), Error> {
        self.cache.remove(template);

        Ok(())
    }

    fn add(&mut self, key: CacheInput, val: Uuid, template: &str) -> Result<(), Error> {
        let map = self.cache
            .get_mut(template)
            .ok_or(Error::DoesNotExist(ItemType::Template(template.into())))?;

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

    fn get(&self, key: CacheInput, template: &str) -> Result<HashSet<Uuid>, Error> {
        match self.cache
            .get(template)
            .ok_or(Error::DoesNotExist(ItemType::Template(template.into())))?
            .get(&key)
        {
            Some(cache) => Ok(cache.clone()),
            None => Ok(HashSet::new()),
        }
    }
}

impl From<TryFromSliceError> for Error {
    fn from(_: TryFromSliceError) -> Self {
        Error::Decode
    }
}

impl From<uuid::Error> for Error {
    fn from(_: uuid::Error) -> Self {
        Error::Decode
    }
}

impl From<EncodeError> for Error {
    fn from(_: EncodeError) -> Self {
        Error::Encode
    }
}

impl From<sled::Error> for Error {
    fn from(_: sled::Error) -> Self {
        Error::CorruptDB
    }
}

impl From<DecodeError> for Error {
    fn from(_: DecodeError) -> Self {
        Error::Decode
    }
}

impl From<serde_json::Error> for Error {
    fn from(_: serde_json::Error) -> Self {
        Error::Decode
    }
}

impl From<crate::data::template::Error> for Error {
    fn from(v: TemplateError) -> Self {
        Error::FormDoesNotFollowTemplate { template: v.template }
    }
}

#[derive(Debug, Display)]
pub enum Error {
    CorruptDB,
    DoesNotExist(ItemType),
    FormDoesNotFollowTemplate{ template: String },
    Decode,
    Encode
}

#[derive(Debug, Display)]
pub enum ItemType {
    Template(String),
    Schedule(String),
    Form(String)
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
