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

pub struct InternalMessage {
    pub id: Uuid,
    pub msg: Internal
}

pub enum Internal {
    Form(FormMessage),
    Template(TemplateMessage),
    Scouter(ScouterMessage),
    Schedule(ScheduleMessage)
}

pub enum ScheduleMessage {
    Add(Schedule),
    Modify(Schedule),
    Remove(String)
}

pub enum ScouterMessage {
    Add(Vec<Scouter>),
    Modify(Scouter),
    Remove(Uuid)
}

pub enum TemplateMessage {
    Add(Vec<FormTemplate>),
    Modify(FormTemplate),
    Remove(String)
}

pub enum FormMessage {
    Add(AddFormData),
    Remove(RemoveFormData)
}

pub struct RemoveFormData {
    pub template: String,
    pub id: Uuid
}

pub struct AddFormData {
    pub template: String,
    pub forms: Vec<Form>
}