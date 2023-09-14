use crate::data::template::FieldDataType::{CheckBox, LongText, Number, Rating, ShortText, Title};
use crate::data::{FieldData, Form};
use serde::{Deserialize, Serialize};
use std::result::Result;

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
            data_type,
        });
    }

    pub fn validate_form(&self, form: &Form) -> Result<(), Error> {
        for x in &self.fields {
            if !matches!(x.data_type, Title) {
                match form.get_field(&x.name) {
                    None => {
                        return Err(Error { template: self.name.clone() });
                    }
                    Some(data) => {
                        if !x.data_type_match(data) {
                            return Err(Error { template: self.name.clone() });
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

impl FieldTemplate {
    fn data_type_match(&self, data: &FieldData) -> bool {
        match data {
            FieldData::CheckBox(_) => self.data_type == CheckBox,
            FieldData::Rating(_) => {
                matches!(self.data_type, Rating { .. })
            }
            FieldData::Number(_) => self.data_type == Number,
            FieldData::ShortText(_) => self.data_type == ShortText,
            FieldData::LongText(_) => self.data_type == LongText,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct FieldTemplate {
    data_type: FieldDataType,
    name: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FormTemplate {
    fields: Vec<FieldTemplate>,
    pub name: String,
    year: i64,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug)]
pub enum FieldDataType {
    Title,
    CheckBox,
    Rating { min: i64, max: i64 },
    Number,
    ShortText,
    LongText,
}

pub struct Error {
    pub template: String
}
