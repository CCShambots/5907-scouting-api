use std::collections::HashMap;
use crate::data::db_layer::{DBLayer, Filter, GetError, SubmitError};
use crate::data::template::FormTemplate;
use crate::data::{Form, Schedule, Shift};
use crate::settings::Settings;
use sled::Db;
use tokio::sync::Mutex;
use tokio::sync::oneshot::{Receiver, Sender};
use uuid::Uuid;
use tokio::time::{sleep, Duration, Interval, interval};
use crate::data::db_layer::GetError::{NoStoredFormsForTemplate, Timeout};

impl AppState {
    pub fn new(db: Db, config: Settings) -> Self {
        Self {
            db_layer: DBLayer::new(db, "templates"),
            config,
            latest_map: Mutex::new(HashMap::new())
        }
    }

    pub async fn get_latest(&self, uuid: Uuid, template: String) -> Result<Vec<(Uuid, Form)>, GetError> {
        println!("recieved req");

        match self.db_layer.get_latest(&template).await? {
            None => {
                println!("no latest, sleeping for 15 secs");
                sleep(Duration::from_secs(15)).await;
                Err(NoStoredFormsForTemplate { template })
            }
            Some(id) => {
                println!("latest found");

                let locked = self.latest_map.lock().await;
                let latest_received = locked.get(&uuid).cloned();
                drop(locked);

                match latest_received {
                    None => {
                        println!("new client, sending all");
                        self.latest_map.lock().await.insert(uuid, id);
                        println!("locked hashmap");
                        Ok(self.db_layer.get_all_pairs(&template)?)
                    }
                    Some(_) => {
                        println!("old client, checking if it has the latest");
                        let mut latest_rec = *self.latest_map.lock().await.get(&uuid).unwrap();
                        let mut interval = interval(Duration::from_secs(5));
                        let mut loops = 0;

                        for _ in 0..24 {
                            let db_latest =
                                self.db_layer.get_latest(&template).await?.unwrap();

                            println!("{} -- {}", latest_rec, db_latest);

                            if latest_rec != db_latest {
                                break;
                            }

                            println!("old client has latest, looping");

                            interval.tick().await;

                            latest_rec = *self.latest_map.lock().await.get(&uuid).unwrap();
                            loops += 1;
                        }

                        if loops == 24 {
                            return Err(Timeout)
                        }

                        println!("sending the from the latest");
                        let latest = self.db_layer.get_latest(&template).await?.unwrap();
                        self.latest_map.lock().await.insert(uuid, latest);

                        self.db_layer.get_range(&template, *self.latest_map.lock().await.get(&uuid).unwrap()..latest)
                    }
                }
            }
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
    config: Settings,
    latest_map: Mutex<HashMap<Uuid, Uuid>>,
}
