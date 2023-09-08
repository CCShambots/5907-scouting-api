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
    Add(AddType, Uuid),
    Remove(RemoveType, Uuid)
}

#[derive(Serialize, Deserialize, Debug)]
pub enum AddType {
    Form(Form),
    Schedule(Schedule),
    Shift(Shift),
    Scouter(Scouter)
}

#[derive(Serialize, Deserialize, Debug)]
pub enum RemoveType {
    Form,
    Schedule,
    Shift,
    Scouter
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