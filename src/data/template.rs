use std::collections::HashSet;
use serde::{Deserialize, Serialize};
use serde_json::{Result, Value};
use crate::data::{FieldData, Form};
use crate::data::template::FieldDataType::{CheckBox, LongText, Number, Rating, ShortText, Title};

impl FormTemplate {
    pub fn validate_form(&self, form: &Form) -> bool {
        for x in &self.fields {
            match form.get_field(&x.name) {
                None => { return false; }
                Some(data) => {
                    if !x.data_type_match(data) { return false; }
                }
            }
        }

        true
    }
}

impl FieldTemplate {
    fn data_type_match(&self, data: &FieldData) -> bool {
        match data {
            FieldData::Title(_) => { self.data_type == Title }
            FieldData::CheckBox(_) => { self.data_type == CheckBox }
            FieldData::Rating(_) => { self.data_type == Rating(_, _) }
            FieldData::Number(_) => { self.data_type == Number(_, _) }
            FieldData::ShortText(_) => { self.data_type == ShortText }
            FieldData::LongText(_) => { self.data_type == LongText }
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct FieldTemplate {
    data_type: FieldDataType,
    name: String
}

#[derive(Serialize, Deserialize)]
pub struct FormTemplate {
    fields: Vec<FieldTemplate>,
    name: String,
    year: i64
}

#[derive(Serialize, Deserialize, Eq, PartialEq)]
enum FieldDataType {
    Title,
    CheckBox,
    Rating(i64, i64),
    Number(i64, i64),
    ShortText,
    LongText
}