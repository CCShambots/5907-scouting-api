pub mod template;

use std::collections::{HashMap, HashSet};
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use serde_json::{Result, Value};

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
    pub event_key: String
}

#[derive(Encode, Decode, Debug, Serialize, Deserialize)]
pub enum FieldData {
    CheckBox(bool),
    Rating(i64),
    Number(i64),
    ShortText(String),
    LongText(String)
}