use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::{Encode, FromRow, Type};
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

#[derive(Serialize, Deserialize, Debug)]
pub struct InternalMessage {
    pub id: Uuid,
    pub data_type: DataType,
    pub action: Action,
    pub new_path: String,
}

#[derive(Serialize, Deserialize, Debug, Type)]
pub enum DataType {
    Bytes,
    Form,
    Schedule,
    Template,
}

#[derive(Serialize, Deserialize, Debug, Type)]
pub enum Action {
    Add,
    Delete,
    Edit,
}
