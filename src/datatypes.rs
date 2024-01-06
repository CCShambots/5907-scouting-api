use crate::auth::GoogleUser;
use axum::async_trait;
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::response::Response;
use datafusion::arrow::array::StringBuilder;
use serde::{Deserialize, Serialize};
use sha256::Sha256Digest;
use std::collections::HashMap;
use std::ops::Add;
use uuid::Uuid;

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

    pub fn validate_form(&self, form: &Form) -> bool {
        for x in &self.fields {
            if !matches!(x.data_type, FieldDataType::Title) {
                match form.get_field(&x.name) {
                    None => return false,
                    Some(data) => {
                        if !x.data_type_match(data) {
                            return false;
                        }
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
            FieldData::CheckBox(_) => self.data_type == FieldDataType::CheckBox,
            FieldData::Rating(_) => {
                matches!(self.data_type, FieldDataType::Rating { .. })
            }
            FieldData::Number(_) => self.data_type == FieldDataType::Number,
            FieldData::ShortText(_) => self.data_type == FieldDataType::ShortText,
            FieldData::LongText(_) => self.data_type == FieldDataType::LongText,
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

impl Form {
    pub fn add_field(&mut self, name: &str, data: FieldData) {
        self.fields.insert(name.into(), data);
    }

    pub fn get_field(&self, name: &str) -> Option<&FieldData> {
        self.fields.get(name)
    }
}

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
pub struct Form {
    fields: HashMap<String, FieldData>,
    pub scouter: String,
    pub team: i64,
    pub match_number: i64,
    pub event_key: String,
    pub id: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Filter {
    pub match_number: Option<i64>,
    pub team: Option<i64>,
    pub event: Option<String>,
    pub scouter: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum FieldData {
    CheckBox(bool),
    Rating(i64),
    Number(i64),
    ShortText(String),
    LongText(String),
}

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
pub struct Schedule {
    pub event: String,
    pub shifts: Vec<Shift>,
}

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
pub struct Shift {
    pub scouter: String,
    pub station: u8,
    pub match_start: u32,
    pub match_end: u32,
}

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
pub struct ItemPath(pub Option<String>);

#[async_trait]
impl<S> FromRequestParts<S> for ItemPath
where
    S: Send + Sync + std::fmt::Debug,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let uri = parts
            .uri
            .path()
            .replace("/age/", "")
            .replace("/protected", "");

        let uri: Vec<&str> = uri.split('/').collect();

        let path = match uri.first() {
            Some(&"") => Some("".into()),
            Some(&"bytes") => match uri.get(1) {
                None => Some("bytes".into()),
                Some(name) => match uri.get(2) {
                    None => Some(format!("bytes/{}.current", name.digest())),
                    Some(ver_id) => Some(format!("bytes/{}.{}", name.digest(), ver_id.digest())),
                },
            },
            Some(&"template" | &"templates") => match uri.get(1) {
                None => Some("templates".into()),
                Some(name) => match uri.get(2) {
                    None => Some(format!("templates/{}.current", name.digest())),
                    Some(ver_id) => {
                        Some(format!("templates/{}.{}", name.digest(), ver_id.digest()))
                    }
                },
            },
            Some(&"schedule" | &"schedules") => match uri.get(1) {
                None => Some("templates".into()),
                Some(name) => match uri.get(2) {
                    None => Some(format!("templates/{}.current", name.digest())),
                    Some(ver_id) => {
                        Some(format!("templates/{}.{}", name.digest(), ver_id.digest()))
                    }
                },
            },
            Some(&"form" | &"forms") => match uri.get(1) {
                None => Some("forms".into()),
                Some(template) => match uri.get(2) {
                    None => None,
                    Some(&"ver") => match uri.get(3) {
                        None => None,
                        Some(template_version) => match uri.get(4) {
                            None => None,
                            Some(form) => match uri.get(5) {
                                None => Some(format!(
                                    "forms/{}.{}/{}.current",
                                    template.digest(),
                                    template_version.digest(),
                                    form.digest()
                                )),
                                Some(form_version) => Some(format!(
                                    "forms/{}.{}/{}.{}",
                                    template.digest(),
                                    template_version.digest(),
                                    form.digest(),
                                    form_version.digest()
                                )),
                            },
                        },
                    },
                    Some(form) => match uri.get(3) {
                        None => Some(format!(
                            "forms/{}.current/{}.current",
                            template.digest(),
                            form.digest()
                        )),
                        Some(form_version) => Some(format!(
                            "forms/{}.current/{}.{}",
                            template.digest(),
                            form.digest(),
                            form_version.digest()
                        )),
                    },
                },
            },
            _ => None,
        };

        Ok(Self(path))
    }
}
