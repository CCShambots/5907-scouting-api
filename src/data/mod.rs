pub mod db_layer;
pub mod template;

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use serde_json::{Result, Value};
use std::collections::{HashMap, HashSet};

impl Form {
    pub fn add_field(&mut self, name: &str, data: FieldData) {
        self.fields.insert(name.into(), data);
    }

    pub fn get_field(&self, name: &str) -> Option<&FieldData> {
        self.fields.get(name)
    }
}

#[derive(Default, Encode, Decode, Debug, Serialize, Deserialize)]
pub struct Form {
    fields: HashMap<String, FieldData>,
    pub scouter: String,
    pub team: i64,
    pub match_number: i64,
    pub event_key: String,
}

#[derive(Encode, Decode, Debug, Serialize, Deserialize)]
pub enum FieldData {
    CheckBox(bool),
    Rating(i64),
    Number(i64),
    ShortText(String),
    LongText(String),
}

#[derive(Default, Encode, Decode, Debug, Serialize, Deserialize, Clone)]
pub struct Schedule {
    pub event: String,
    pub shifts: Vec<Shift>,
}

#[derive(Default, Encode, Decode, Debug, Serialize, Deserialize, Clone)]
pub struct Shift {
    pub scouter: String,
    pub station: u8,
    pub match_start: u32,
    pub match_end: u32,
}
