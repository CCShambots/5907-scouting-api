use std::collections::HashSet;
use serde::{Deserialize, Serialize};
use serde_json::{Result, Value};
use crate::data::{FieldData, Form};
use crate::data::template::FieldDataType::{CheckBox, LongText, Number, Rating, ShortText, Title};

impl FormTemplate {
    pub fn new(name: &str, year: i64) -> Self {
        Self {
            fields: vec![],
            name: name.into(),
            year,
        }
    }

    pub fn add_field(&mut self, name: &str, data_type: FieldDataType) {
        self.fields.push(FieldTemplate {
            name: name.into(),
            data_type
        });
    }

    pub fn validate_form(&self, form: &Form) -> bool {
        for x in &self.fields {
            if !matches!(x.data_type, Title) {
                match form.get_field(&x.name) {
                    None => { return false; }
                    Some(data) => {
                        if !x.data_type_match(data) { return false; }
                    }
                }
            }
        }

        true
    }
}

impl FieldTemplate {
    fn data_type_match(&self, data: &FieldData) -> bool {
        match data {
            FieldData::CheckBox(_) => { self.data_type == CheckBox }
            FieldData::Rating(_) => { matches!(self.data_type, Rating{ .. }) }
            FieldData::Number(_) => { self.data_type == Number }
            FieldData::ShortText(_) => { self.data_type == ShortText }
            FieldData::LongText(_) => { self.data_type == LongText }
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct FieldTemplate {
    data_type: FieldDataType,
    name: String
}

#[derive(Serialize, Deserialize, Clone)]
pub struct FormTemplate {
    fields: Vec<FieldTemplate>,
    pub name: String,
    year: i64
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone)]
pub enum FieldDataType {
    Title,
    CheckBox,
    Rating{ min: i64, max: i64 },
    Number,
    ShortText,
    LongText
}