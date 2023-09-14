use crate::data::db_layer::CacheInput::{Event, Match, Team};
use crate::data::template::{FormTemplate};
use crate::data::{Form, Schedule, Scouter, Shift};
use actix_web::body::BoxBody;
use actix_web::http::header::ContentType;
use actix_web::http::StatusCode;
use actix_web::{HttpResponse, ResponseError};
use bincode::config::{Configuration};
use bincode::error::{DecodeError, EncodeError};
use derive_more::{Display, Error};
use serde::{Deserialize, Serialize};
use sled::{Batch, Db, IVec, Transactional};
use std::array::TryFromSliceError;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::fs::read_to_string;
use std::io::Read;
use std::ops::BitAnd;
use std::path::Path;
use std::sync::Arc;
use serde_json::json;
use tokio::sync::{Mutex, RwLock};
use uuid::{Error as UuidError, Uuid};
use crate::data::db_layer::Error::{ExistsAlready, TemplateHasForms};
use crate::data::template::Error as TemplateError;
use crate::logic::messages::{AddType, EditType, RemoveType};


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
                "templates".into(),
                "raw_storage".into()
            ])
        }
    }

    pub async fn add(&self, d_type: &AddType) -> Result<String, Error> {
        match d_type {
            AddType::Form(form, template) => self.add_form(form, template).await,
            AddType::Schedule(schedule) => self.add_schedule(schedule).await,
            AddType::Shift(shift, event) => self.add_shift(shift, event).await,
            AddType::Scouter(scouter) => self.add_scouter(scouter).await,
            AddType::Bytes(bytes, key) => self.add_bytes(bytes, key).await,
            AddType::Template(template) => self.add_template(template).await
        }
    }

    pub async fn edit(&self, d_type: &EditType) -> Result<String, Error> {
        match d_type {
            EditType::Form(form, id, template) => self.edit_form(form, *id, template).await,
            EditType::Schedule(schedule) => self.edit_schedule(schedule).await,
            EditType::Scouter(scouter) => self.edit_scouter(scouter).await,
            EditType::Bytes(bytes, key) => self.edit_bytes(bytes, key).await,
            EditType::Template(template) => self.edit_template(template).await
        }
    }

    pub async fn remove(&self, d_type: &RemoveType) -> Result<String, Error> {
        match d_type {
            RemoveType::Form(template, id) => self.remove_form(template, *id).await,
            RemoveType::Schedule(event) => self.remove_schedule(event).await,
            RemoveType::Shift(event, idx) => self.remove_shift(event, *idx).await,
            RemoveType::Scouter(key) => self.remove_scouter(key).await,
            RemoveType::Bytes(key) => self.remove_bytes(key).await,
            RemoveType::Template(name) => self.remove_template(name).await
        }
    }

    async fn remove_template(&self, name: &str) -> Result<String, Error> {
        let tree = self.db.open_tree("templates")?;

        match tree.contains_key(name)? {
            false => Err(Error::DoesNotExist(ItemType::Template(name.into()))),
            true => {
                let old: FormTemplate = serde_cbor::from_slice(&tree.remove(name)?.unwrap())?;
                self.db.drop_tree(name)?;

                Ok(Self::jser(&old)?)
            }
        }
    }

    async fn remove_bytes(&self, key: &str) -> Result<String, Error> {
        let tree = self.db.open_tree("raw_storage")?;

        match tree.contains_key(key)? {
            false => Err(Error::DoesNotExist(ItemType::Bytes(key.into()))),
            true => {
                self.db.remove(key)?;

                Ok(String::new())
            }
        }
    }

    async fn remove_scouter(&self, key: &str) -> Result<String, Error> {
        let tree = self.db.open_tree("scouters")?;

        match tree.contains_key(key)? {
            false => Err(Error::DoesNotExist(ItemType::Scouter(key.into()))),
            true => {
                let old: Scouter = serde_cbor::from_slice(&tree.remove(key)?.unwrap())?;

                Ok(Self::jser(&old)?)
            }
        }
    }

    async fn remove_shift(&self, event: &str, idx: u64) -> Result<String, Error> {
        let tree = self.db.open_tree("schedules")?;

        match tree.contains_key(event)? {
            false => Err(Error::DoesNotExist(ItemType::Schedule(event.into()))),
            true => {
                let mut schedule: Schedule = serde_cbor::from_slice(&tree.get(event)?.unwrap())?;

                if idx > schedule.shifts.len() as u64 - 1 {
                    return Err(Error::DoesNotExist(ItemType::Shift(event.into(), idx)));
                }

                let old = schedule.shifts[idx as usize].clone();

                schedule.shifts.remove(idx as usize);

                Ok(Self::jser(&old)?)
            }
        }
    }

    async fn remove_schedule(&self, event: &str) -> Result<String, Error> {
        let tree = self.db.open_tree("schedules")?;

        match tree.contains_key(event)? {
            false => Err(Error::DoesNotExist(ItemType::Schedule(event.into()))),
            true => {
                let old: Schedule = serde_cbor::from_slice(&tree.remove(event)?.unwrap())?;

                Ok(Self::jser(&old)?)
            }
        }
    }

    async fn remove_form(&self, template: &str, id: Uuid) -> Result<String, Error> {
        let temp = self.get_template(template).await?;
        let tree = self.db.open_tree(temp.name)?;

        match tree.contains_key(id)? {
            false => Err(Error::DoesNotExist(ItemType::Form(template.into(), id))),
            true => {
                let old: Form = serde_cbor::from_slice(&tree.remove(id)?.unwrap())?;

                Ok(Self::jser(&old)?)
            }
        }
    }

    async fn edit_template(&self, template: &FormTemplate) -> Result<String, Error> {
        let tree = self.db.open_tree("templates")?;

        match tree.contains_key(&template.name)? {
            false => Err(Error::DoesNotExist(ItemType::Template(template.name.clone()))),
            true => {
                if !self.db.open_tree(&template.name)?.is_empty() {
                    return Err(TemplateHasForms { template: template.name.clone() })
                }

                let old: FormTemplate = serde_cbor::from_slice(&tree.insert(&template.name, Self::ser(&template)?)?.unwrap())?;

                Ok(Self::jser(&(old, &template.name))?)
            }

        }
    }

    async fn edit_bytes(&self, bytes: &[u8], key: &str) -> Result<String, Error> {
        let tree = self.db.open_tree("raw_storage")?;

        match tree.contains_key(key)? {
            false => Err(Error::DoesNotExist(ItemType::Bytes(key.into()))),
            true => {
                tree.insert(key, bytes)?;

                Ok(Self::jser(&((), key))?)
            }
        }
    }

    async fn edit_scouter(&self, scouter: &Scouter) -> Result<String, Error> {
        let tree = self.db.open_tree("scouters")?;
        let key = Self::scouter_key(scouter);

        match tree.contains_key(&key)? {
            false => Err(Error::DoesNotExist(ItemType::Scouter(key))),
            true => {
                let old = serde_cbor::from_slice(&tree.insert(&key, Self::ser(scouter)?)?.unwrap())?;

                Ok(Self::jser(&(old, key))?)
            }
        }
    }

    async fn edit_shift(&self, event: &str, idx: u64, shift: &Shift) -> Result<String, Error> {
        let tree = self.db.open_tree("schedules")?;

        match tree.contains_key(&event)? {
            false => Err(Error::DoesNotExist(ItemType::Schedule(event.into()))),
            true => {
                let mut schedule: Schedule = serde_cbor::from_slice(&tree.get(&event)?.unwrap())?;

                if idx > schedule.shifts.len() as u64 - 1 {
                    return Err(Error::DoesNotExist(ItemType::Shift(event.into(), idx)))
                }

                let old = schedule.shifts[idx as usize].clone();

                schedule.shifts[idx as usize] = shift.clone();

                Ok(Self::jser(&(old, (event, idx)))?)
            }
        }
    }

    async fn edit_schedule(&self, schedule: &Schedule) -> Result<String, Error> {
        let tree = self.db.open_tree("schedules")?;

        match tree.contains_key(&schedule.event)? {
            false => Err(Error::DoesNotExist(ItemType::Schedule(schedule.event.clone()))),
            true => {
                let old = serde_cbor::from_slice(&tree.insert(&schedule.event, Self::ser(schedule)?)?.unwrap())?;

                Ok(Self::jser(&(old, &schedule.event))?)
            }

        }
    }

    async fn edit_form(&self, form: &Form, id: Uuid, template: &str) -> Result<String, Error> {
        let temp = self.get_template(template).await?;
        temp.validate_form(form)?;
        let tree = self.db.open_tree(temp.name)?;

        match tree.contains_key(id)? {
            false => Err(Error::DoesNotExist(ItemType::Form(template.into(), id))),
            true => {
                let old: Form = serde_cbor::from_slice(&tree.insert(id, Self::ser(form)?)?.unwrap())?;
                self.update_cache(form, id, template).await?;

                Ok(Self::jser(&(old, id))?)
            }
        }
    }

    async fn add_template(&self, template: &FormTemplate) -> Result<String, Error> {
        let tree = self.db.open_tree("templates")?;

        match self.template_exists(&template.name) {
            true => Err(Error::ExistsAlready(ItemType::Template(template.name.clone()))),
            false => {
                tree.insert(&template.name, Self::ser(template)?)?;
                self.cache.write().await.add_template(&template.name)?;

                Ok(Self::jser(&template.name)?)
            }
        }
    }

    async fn add_bytes(&self, bytes: &Vec<u8>, key: &str) -> Result<String, Error> {
        let tree = self.db.open_tree("raw_storage")?;

        match tree.contains_key(key)? {
            true => Err(ExistsAlready(ItemType::Bytes(key.into()))),
            false => {
                tree.insert(key, &bytes[..])?;
                Ok(Self::jser(&key.to_string())?)
            }
        }
    }

    async fn add_scouter(&self, scouter: &Scouter) -> Result<String, Error> {
        let tree = self.db.open_tree("scouters")?;
        let scouter_key = Self::scouter_key(scouter);

        match tree.contains_key(&scouter_key)? {
            true => Err(Error::ExistsAlready(ItemType::Scouter(scouter_key))),
            false => {
                tree.insert(&scouter_key, Self::ser(scouter)?)?;
                Ok(Self::jser(&scouter_key)?)
            }
        }
    }

    async fn add_shift(&self, shift: &Shift, event: &str) -> Result<String, Error> {
        let mut schedule = self.get_schedule(event).await?;
        let tree = self.db.open_tree(&schedule.event)?;

        match tree.contains_key(event)? {
            true => Err(Error::DoesNotExist(ItemType::Schedule(event.into()))),
            false => {
                schedule.shifts.push(shift.clone());
                tree.insert(event, Self::ser(&schedule)?)?;
                Ok(Self::jser(&(event.to_string(), schedule.shifts.len() - 1))?)
            }
        }
    }

    async fn add_schedule(&self, schedule: &Schedule) -> Result<String, Error> {
        let tree = self.db.open_tree("schedules")?;

        match tree.contains_key(&schedule.event)? {
            true => Err(Error::ExistsAlready(ItemType::Schedule(schedule.event.clone()))),
            false => {
                tree.insert(&schedule.event, Self::ser(schedule)?)?;
                Ok(Self::jser(&schedule.event)?)
            }
        }
    }

    async fn add_form(&self, form: &Form, template: &str) -> Result<String, Error> {
        let temp = self.get_template(template).await?;
        temp.validate_form(form)?;
        let tree = self.db.open_tree(temp.name)?;
        let id = Uuid::new_v4();

        match tree.contains_key(id)? {
            true => Err(Error::ExistsAlready(ItemType::Form(template.into(), id))),
            false => {
                tree.insert(id, Self::ser(form)?)?;
                self.update_cache(form, id, template).await?;
                Ok(Self::jser(&id)?)
            }
        }
    }



    pub async fn get(&self, template: String, filter: Filter) -> Result<Vec<(Form, Uuid)>, Error> {
        let cache_result = self.get_uuids_from_filter(&template, &filter).await?;
        let mut set = cache_result[0].clone();

        for x in &cache_result[1..] {
            set = set.bitand(x);
        }

        self.get_forms(&template, set.iter().collect())
    }

    pub async fn get_form(&self, template: &str, uuid: Uuid) -> Result<(Form, Uuid), Error> {
        let tree = self.db.open_tree(template)?;

        if !tree.contains_key(uuid)? {
            return Err(Error::DoesNotExist(ItemType::Form(template.into(), uuid)))
        }

        Ok((serde_cbor::de::from_slice(&tree.get(uuid)?.unwrap())?, uuid))
    }

    fn get_forms(&self, template: &String, uuids: Vec<&Uuid>) -> Result<Vec<(Form, Uuid)>, Error> {
        let tree = self.db.open_tree(template)?;

        Ok(
            uuids
                .iter()
                .map(|x| (serde_cbor::de::from_slice(&tree.get(x).unwrap().unwrap()).unwrap(), **x))
                .collect()
        )
    }

    pub async fn get_schedule(&self, event: &str) -> Result<Schedule, Error> {
        let tree = self.db.open_tree("schedules")?;

        if !tree.contains_key(event)? {
            return Err(Error::DoesNotExist(ItemType::Schedule(event.into())));
        }

        let decoded: Schedule = serde_cbor::de::from_slice(&tree.get(event)?.unwrap())?;

        Ok(decoded)
    }

    pub async fn get_bytes_by_key(&self, key: &str) -> Result<Vec<u8>, Error> {
        let tree = self.db.open_tree("raw_storage")?;

        match tree.get(key)? {
            None => Err(Error::DoesNotExist(ItemType::Scouter(key.into()))),
            Some(ser) => Ok(ser.to_vec())
        }
    }

    pub async fn get_bytes(&self) -> Result<Vec<String>, Error> {
        Ok(self.db.open_tree("raw_storage")?
            .iter()
            .keys()
            .map(|k| String::from_utf8(k.unwrap().to_vec()).unwrap())
            .collect())
    }

    pub async fn get_schedule_events(&self) -> Result<Vec<String>, Error> {
        Ok(self.db.open_tree("schedules")?
            .iter()
            .keys()
            .map(|k| String::from_utf8(k.unwrap().to_vec()).unwrap())
            .collect())
    }

    pub async fn get_scouter(&self, key: &str) -> Result<Scouter, Error> {
        let tree = self.db.open_tree("scouters")?;

        match tree.get(key)? {
            None => Err(Error::DoesNotExist(ItemType::Scouter(key.into()))),
            Some(ser) => Ok(serde_cbor::from_slice(&ser)?)
        }
    }

    pub async fn get_scouters(&self) -> Result<Vec<String>, Error> {
        Ok(self.db.open_tree("scouters")?
            .iter()
            .keys()
            .map(|k| String::from_utf8(k.unwrap().to_vec()).unwrap())
            .collect())
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
            Some(temp) => Ok(serde_cbor::from_slice(&temp)?),
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
            out.push(lock.get(CacheInput::Scouter(scouter.clone()), template)?);
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
                    let form: Form = serde_cbor::from_slice(&pair.1)?;


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
        lock.add(CacheInput::Scouter(form.scouter.clone()), uuid, template)?;
        lock.add(Event(form.event_key.clone()), uuid, template)?;

        Ok(())
    }

    fn ser(s: &impl Serialize) -> Result<Vec<u8>, Error> {
        serde_cbor::to_vec(s).map_err(|err| { err.into() })
    }

    fn jser(s: &impl Serialize) -> Result<String, Error> {
        serde_json::to_string(s).map_err(|err| { err.into() })
    }

    fn scouter_key(sc: &Scouter) -> String {
        format!("{}{}", sc.name, sc.team)
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

impl From<serde_cbor::Error> for Error {
    fn from(value: serde_cbor::Error) -> Self {
        Error::Encode
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
    ExistsAlready(ItemType),
    FormDoesNotFollowTemplate{ template: String },
    TemplateHasForms{ template: String },
    Decode,
    Encode
}

impl Display for ItemType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ItemType::Template(name) => write!(f, "Template: {}", name),
            ItemType::Schedule(event) => write!(f, "Schedule: {}", event),
            ItemType::Shift(event, idx) => write!(f, "Shift: {}, Index: {}", event, idx),
            ItemType::Form(template, id) => write!(f, "Form: {}, ID: {}", template, id),
            ItemType::Bytes(key) => write!(f, "Bytes: {}", key),
            ItemType::Scouter(scouter) => write!(f, "Scouter: {}", scouter)
        }
    }
}

#[derive(Debug)]
pub enum ItemType {
    Template(String),
    Schedule(String),
    Shift(String, u64),
    Scouter(String),
    Form(String, Uuid),
    Bytes(String)
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
