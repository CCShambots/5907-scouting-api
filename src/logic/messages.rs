use serde::{Deserialize, Serialize};
use uuid::Uuid;
use crate::data::{Form, Schedule, Scouter, Shift};

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

#[derive(Serialize, Deserialize, Debug)]
pub enum AddType {
    Form(Form, String),
    Schedule(Schedule),
    Shift(Shift, String),
    Scouter(Scouter),
    Bytes(Vec<u8>, String)
}

#[derive(Serialize, Deserialize, Debug)]
pub enum EditType {
    Form(Form, String),
    Schedule(Schedule),
    Scouter(Scouter),
    Shift(String, u64, Shift),
    Bytes(Vec<u8>, String)
}

#[derive(Serialize, Deserialize, Debug)]
pub enum RemoveType {
    Form(Uuid),
    Schedule(String),
    Shift(String, u64),
    Scouter(String),
    Bytes(String)
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