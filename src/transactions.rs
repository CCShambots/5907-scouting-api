use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::{Encode, FromRow, Type};
use uuid::Uuid;

impl Transaction {
    pub fn new(data_type: DataType, action: Action, blob_id: Uuid, alt_key: String) -> Self {
        Self {
            id: Uuid::new_v4(),
            data_type,
            action,
            blob_id,
            timestamp: Utc::now().timestamp_micros(),
            alt_key,
        }
    }

    pub fn describe(&self) -> String {
        format!("[{:?}] of [{:?}] object with key [{}]", self.action, self.data_type, self.alt_key)
    }
}

#[derive(Serialize, Deserialize, FromRow, Encode, Debug)]
pub struct Transaction {
    pub id: Uuid,
    pub data_type: DataType,
    pub action: Action,
    pub blob_id: Uuid,
    pub alt_key: String,
    pub timestamp: i64,
}

#[derive(Serialize, Deserialize, Debug, Type, Copy, Clone)]
pub enum DataType {
    Bytes,
    Form,
    Schedule,
    Template,
}

#[derive(Serialize, Deserialize, Debug, Type, Copy, Clone)]
pub enum Action {
    Add,
    Delete,
    Edit,
}
