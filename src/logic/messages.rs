use serde::{Deserialize, Serialize};
use uuid::Uuid;
use crate::data::{Form, Schedule, Scouter, Shift};
use crate::data::template::FormTemplate;

impl InternalMessage {
    pub fn new(msg: Internal) -> Self {
        Self {
            msg,
            id: Uuid::new_v4()
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct InternalMessage {
    pub id: Uuid,
    pub msg: Internal
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Internal {
    Add(AddType),
    Remove(RemoveType),
    Edit(EditType)
}

impl AddType {
    pub fn get_type(&self) -> String {
        match self {
            AddType::Form(_, _) => { "Form".into() }
            AddType::Schedule(_) => { "Schedule".into() }
            AddType::Shift(_, _) => { "Shift".into() }
            AddType::Scouter(_) => { "Scouter".into() }
            AddType::Bytes(_, _) => { "Bytes".into() }
            AddType::Template(_) => { "Template".into() }
        }
    }
}

impl EditType {
    pub fn get_type(&self) -> String {
        match self {
            EditType::Form(_, _, _) => { "Form".into() }
            EditType::Schedule(_) => { "Schedule".into() }
            EditType::Scouter(_) => { "Scouter".into() }
            EditType::Bytes(_, _) => { "Bytes".into() }
            EditType::Template(_) => { "Template".into() }
        }
    }
}

impl RemoveType {
    pub fn get_type(&self) -> String {
        match self {
            RemoveType::Form(_, _) => { "Form".into() }
            RemoveType::Schedule(_) => { "Schedule".into() }
            RemoveType::Shift(_, _) => { "Shift".into() }
            RemoveType::Scouter(_) => { "Scouter".into() }
            RemoveType::Bytes(_) => { "Bytes".into() }
            RemoveType::Template(_) => { "Template".into() }
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum AddType {
    Form(Form, String),
    Schedule(Schedule),
    Shift(Shift, String),
    Scouter(Scouter),
    Bytes(Vec<u8>, String),
    Template(FormTemplate)
}

#[derive(Serialize, Deserialize, Debug)]
pub enum EditType {
    Form(Form, Uuid, String),
    Schedule(Schedule),
    Scouter(Scouter),
    Bytes(Vec<u8>, String),
    Template(FormTemplate)
}

#[derive(Serialize, Deserialize, Debug)]
pub enum RemoveType {
    Form(String, Uuid),
    Schedule(String),
    Shift(String, u64),
    Scouter(String),
    Bytes(String),
    Template(String)
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RemoveFormData {
    pub template: String,
    pub id: Uuid
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AddFormData {
    pub template: String,
    pub forms: Vec<Form>
}