use std::collections::HashSet;
use serde_json::{Result, Value};

impl FormTemplate {
    fn new(name: String) -> Self {
        Self {
            fields: Vec::new(),
            field_names: HashSet::new(),
            name
        }
    }

    fn add_field(&mut self, field: FieldTemplate) -> bool {
        if self.field_names.contains(&field.name) {
            return false;
        }

        self.fields.push(field);

        true
    }

    fn parse_completed(&self, data: &str) -> Result<Form> {
        let json_parse: Value = serde_json::from_str(data)?;
        let mut out = Form::default();

        for field in &self.fields {
            let val: &Value = &json_parse[&field.name];

            out.add_field(Field::new(field.name.clone(), FieldData::from(val)));
        }

        Ok(out)
    }
}

impl Form {
    fn add_field(&mut self, field: Field) {
        self.fields.push(field)
    }
}

impl Field {
    fn new(name: String, data: FieldData) -> Self {
        Self {
            name,
            data
        }
    }
}

impl From<&Value> for FieldData {
    fn from(value: &Value) -> Self {
        if value.is_string() {
            Self::Text(value.to_string())
        }
        else if value.is_number() {
            Self::Number(value.as_i64().unwrap())
        }
        else if value.is_boolean() {
            Self::CheckBox(value.as_bool().unwrap())
        }
        else {
            //error case
            Self::Text("".into())
        }
    }
}

struct FieldTemplate {
    data_type: FieldDataType,
    name: String
}

struct Field {
    data: FieldData,
    name: String
}

#[derive(Default)]
struct Form {
    fields: Vec<Field>
}

struct FormTemplate {
    fields: Vec<FieldTemplate>,
    field_names: HashSet<String>,
    name: String
}

enum FieldData {
    CheckBox(bool),
    Number(i64),
    Text(String)
}

enum FieldDataType {
    Title,
    CheckBox,
    Rating(i64, i64),
    Number(i64, i64),
    ShortText,
    LongText
}