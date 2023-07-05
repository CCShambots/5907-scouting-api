use serde::{Deserialize, Serialize};
use uuid::Uuid;
use crate::data::{Form, Schedule, Scouter};
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
    Form(FormMessage),
    Template(TemplateMessage),
    Scouter(ScouterMessage),
    Schedule(ScheduleMessage)
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ScheduleMessage {
    Add(Schedule),
    Modify(Schedule),
    Remove(String)
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ScouterMessage {
    Add(Vec<Scouter>),
    Modify(Scouter),
    Remove(Uuid)
}

#[derive(Serialize, Deserialize, Debug)]
pub enum TemplateMessage {
    Add(Vec<FormTemplate>),
    Modify(FormTemplate),
    Remove(String)
}

#[derive(Serialize, Deserialize, Debug)]
pub enum FormMessage {
    Add(AddFormData),
    Remove(RemoveFormData)
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