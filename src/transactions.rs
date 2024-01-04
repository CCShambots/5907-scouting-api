use serde::{Deserialize, Serialize};
use uuid::Uuid;

impl InternalMessage {
    pub fn new(data_type: DataType, action: Action, new_path: String) -> Self {
        Self {
            data_type,
            action,
            new_path,
            id: Uuid::new_v4(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct InternalMessage {
    pub id: Uuid,
    pub data_type: DataType,
    pub action: Action,
    pub new_path: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum DataType {
    Bytes,
    Form(String),
    Schedule,
    Scouter,
    Template,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Action {
    Add,
    Delete,
    Edit,
}
