use uuid::Uuid;
use crate::data::{Form, Schedule, Scouter};
use crate::data::template::FormTemplate;

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
    Remove(Uuid)
}

pub struct AddFormData {
    template: String,
    form: Vec<Form>
}