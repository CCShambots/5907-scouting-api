pub mod db_layer;
pub mod template;

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Result, Value};
use std::collections::{HashMap, HashSet};
use bincode::enc::Encoder;
use bincode::enc::write::Writer;
use bincode::error::EncodeError;
use uuid::Uuid;

impl Form {
    pub fn add_field(&mut self, name: &str, data: FieldData) {
        self.fields.insert(name.into(), data);
    }

    pub fn get_field(&self, name: &str) -> Option<&FieldData> {
        self.fields.get(name)
    }
}

#[derive(Default, Encode, Decode, Debug, Serialize, Deserialize, Clone)]
pub struct Form {
    fields: HashMap<String, FieldData>,
    pub scouter: String,
    pub team: i64,
    pub match_number: i64,
    pub event_key: String,
    id: Option<String>
}

#[derive(Encode, Decode, Debug, Serialize, Deserialize, Clone)]
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
    pub shifts: Vec<Shift>
}

#[derive(Default, Encode, Decode, Debug, Serialize, Deserialize, Clone)]
pub struct Shift {
    pub scouter: String,
    pub station: u8,
    pub match_start: u32,
    pub match_end: u32,
}

#[derive(Encode, Decode, Debug, Serialize, Deserialize, Clone)]
pub struct Scouter {
    name: String,
    team: i32,
    accuracy: f32
}