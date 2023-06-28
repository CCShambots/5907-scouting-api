use crate::data::db_layer::{DBLayer, Filter, GetError, SubmitError};
use crate::data::template::FormTemplate;
use crate::data::{Form, Schedule, Shift};
use sled::Db;
use crate::settings::Settings;

impl AppState {
    pub fn new(db: Db, config: Settings) -> Self {
        Self {
            db_layer: DBLayer::new(db, "templates"),
            config
        }
    }

    pub async fn get_templates(&self) -> Vec<String> {
        self.db_layer.get_templates().await
    }

    pub async fn get_template(&self, template: String) -> Result<FormTemplate, GetError> {
        self.db_layer.get_template(template).await
    }

    pub async fn build_cache(&self) -> Result<(), GetError> {
        self.db_layer.build_cache().await
    }

    pub async fn get(&self, template: String, filter: Filter) -> Result<Vec<Form>, GetError> {
        self.db_layer.get(template, filter).await
    }

    pub async fn submit_form(&self, template: String, form: &Form) -> Result<(), SubmitError> {
        self.db_layer.submit_form(template, form).await
    }

    pub async fn get_schedule(&self, event: String) -> Result<Schedule, GetError> {
        self.db_layer.get_schedule(event).await
    }

    pub async fn set_schedule(&self, event: String, schedule: Schedule) -> Result<(), SubmitError> {
        self.db_layer.set_schedule(event, schedule).await
    }

    pub async fn get_shifts(&self, event: String, scouter: String) -> Result<Vec<Shift>, GetError> {
        self.db_layer.get_shifts(event, scouter).await
    }
}

pub struct AppState {
    db_layer: DBLayer,
    config: Settings
}
