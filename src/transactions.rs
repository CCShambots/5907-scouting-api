use crate::datatypes::FormTemplate;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

impl InternalMessage {
    pub fn new(msg: Internal) -> Self {
        Self {
            msg,
            id: Uuid::new_v4(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct InternalMessage {
    pub id: Uuid,
    pub msg: Internal,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Internal {
    Add(AddType),
    Remove(RemoveType),
    Edit(EditType),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum AddType {
    /*Form(Form, String),
    Schedule(Schedule),
    Scouter(Scouter),*/
    Bytes(String),
    Template(FormTemplate),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum EditType {
    /*Form(Form, Uuid, String),
    Schedule(Schedule),
    Scouter(Scouter),*/
    Bytes(String, String),
    Template(FormTemplate),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum RemoveType {
    /*Form(String, Uuid),
    Schedule(String),
    Shift(String, u64),
    Scouter(String),*/
    Bytes(String, String),
    Template(String),
}

/*#[derive(Serialize, Deserialize, Debug)]
pub struct RemoveFormData {
    pub template: String,
    pub id: Uuid
}*/

/*#[derive(Serialize, Deserialize, Debug)]
pub struct AddFormData {
    pub template: String,
    pub forms: Vec<Form>
}*/
