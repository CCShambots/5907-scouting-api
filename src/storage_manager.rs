use crate::datatypes::{DBForm, Filter, Form, FormTemplate, Schedule, StorableObject};
use crate::transactions::{Action, DataType, Transaction};
use anyhow::{anyhow, Error};
use chrono::Utc;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::datatypes;
use datafusion::arrow::datatypes::{Field, FieldRef, Schema, SchemaRef};
use datafusion::arrow::json::writer::record_batches_to_json_rows;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::{col, lit, max, DataFrame, NdJsonReadOptions, SessionContext};
use futures::StreamExt;
use glob::glob;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha256::Sha256Digest;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{query, Executor, QueryBuilder, Row, Sqlite, SqlitePool, Execute};
use std::ops::Add;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::{fs, io};
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

pub const TRANSACTION_TABLE: &str = &"transactions";
pub const FORMS_TABLE: &str = &"forms";

pub struct StorageManager {
    path: String,
    pool: SqlitePool,
}

impl StorageManager {
    pub async fn new(base_path: &str) -> Result<Self, anyhow::Error> {
        Ok(Self {
            path: format!("{base_path}blobs/"),
            pool: SqlitePoolOptions::new()
                .connect(&format!("sqlite://{base_path}database.db"))
                .await?,
        })
    }

    #[instrument(ret, skip(self, data))]
    async fn write_blob(&self, data: impl AsRef<[u8]>) -> Result<Uuid, anyhow::Error> {
        let id: Uuid = Uuid::new_v4();

        write_non_create(format!("{}{}", self.path, id.to_string().digest()), data).await?;

        Ok(id)
    }

    #[instrument(skip(self))]
    async fn read_blob(&self, id: Uuid) -> Result<Vec<u8>, anyhow::Error> {
        fs::read(format!("{}{}", self.path, id.to_string().digest()))
            .await
            .map_err(Into::into)
    }

    #[instrument(skip(self, form))]
    async fn write_form(&self, form: DBForm) -> Result<(), anyhow::Error> {
        let mut query: QueryBuilder<Sqlite> = QueryBuilder::new(format!(
            "INSERT INTO {FORMS_TABLE} (blob_id, team, match_number, event_key, template) VALUES(?, ?, ?, ?, ?)"
        ));
        let mut query = query.build();

        query = query.bind(form.blob_id);
        query = query.bind(form.team);
        query = query.bind(form.match_number);
        query = query.bind(form.event_key);
        query = query.bind(form.template);

        if self.check_form_exists(form.blob_id).await? {
            self.remove_form(form.blob_id).await?;
        }

        self.pool.execute(query).await?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn check_form_exists(&self, blob_id: Uuid) -> Result<bool, anyhow::Error> {
        let mut query_builder: QueryBuilder<Sqlite> = QueryBuilder::new(format!(
            "SELECT COUNT(*) AS count FROM {FORMS_TABLE} WHERE blob_id = ?"
        ));
        let mut query = query_builder.build();

        query = query.bind(blob_id);

        let count: i64 = query
            .fetch_one(&self.pool)
            .await?
            .try_get("count")?;

        Ok(count > 0)
    }

    #[instrument(skip(self))]
    async fn remove_form(&self, blob_id: Uuid) -> Result<(), anyhow::Error> {
        let mut query_builder: QueryBuilder<Sqlite> =
            QueryBuilder::new(format!("DELETE FROM {FORMS_TABLE} WHERE blob_id = ?"));
        let mut query = query_builder.build();

        query = query.bind(blob_id);

        self.pool.execute(query).await?;

        Ok(())
    }

    #[instrument(skip(self, transaction), ret)]
    async fn write_transaction(&self, transaction: Transaction) -> Result<(), anyhow::Error> {
        let mut query_builder: QueryBuilder<Sqlite> = QueryBuilder::new(format!(
            "INSERT INTO {TRANSACTION_TABLE} VALUES (?, ?, ?, ?, ?, ?)"
        ));
        let mut query = query_builder.build();

        let desc = transaction.describe();

        query = query.bind(transaction.id);
        query = query.bind(transaction.data_type);
        query = query.bind(transaction.action);
        query = query.bind(transaction.blob_id);
        query = query.bind(transaction.alt_key);
        query = query.bind(transaction.timestamp);

        self.pool.execute(query).await?;

        info!("{}", desc);

        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn blob_exists(&self, id: Uuid) -> Result<bool, anyhow::Error> {
        fs::try_exists(format!("{}{}", self.path, id.to_string().digest()))
            .await
            .map_err(Into::into)
    }

    #[instrument(skip(self))]
    pub async fn blob_deleted(&self, id: Uuid) -> Result<bool, anyhow::Error> {
        let res: Action =
            sqlx::query("SELECT action FROM transactions WHERE blob_id = ? ORDER BY timestamp DESC")
                .bind(id)
                .fetch_one(&self.pool)
                .await?
                .try_get("action")?;

        Ok(matches!(res, Action::Delete))
    }

    #[instrument(skip(self))]
    pub async fn latest_blob_from_alt_key(
        &self,
        alt_key: &str,
        data_type: DataType,
    ) -> Result<Uuid, anyhow::Error> {
        let res: Uuid = sqlx::query("SELECT blob_id FROM transactions WHERE alt_key = ? AND data_type = ? ORDER BY timestamp DESC")
            .bind(alt_key)
            .bind(data_type)
            .fetch_one(&self.pool).await?
            .try_get("blob_id")?;

        Ok(res)
    }

    #[instrument(skip(self, form))]
    pub async fn forms_add(&self, template: String, form: Form) -> Result<Uuid, anyhow::Error> {
        let id = Uuid::new_v4();
        let mut form = form;
        form.id = Some(id);
        let blob_id = self.write_blob(serde_json::to_string(&form)?).await?;
        let db_form = form.to_db_form(blob_id, template);
        let transaction =
            Transaction::new(DataType::Form, Action::Add, blob_id, id.to_string());

        self.write_form(db_form).await?;
        self.write_transaction(transaction).await?;

        Ok(id)
    }

    #[instrument(skip(self))]
    async fn get_blob_id(
        &self,
        alt_key: &str,
        data_type: DataType,
    ) -> Result<Option<Uuid>, anyhow::Error> {
        let blob_id = self.latest_blob_from_alt_key(alt_key, data_type).await;

        match blob_id {
            Ok(id) => {
                if self.blob_exists(id).await? && !self.blob_deleted(id).await? {
                    Ok(Some(id))
                } else {
                    Ok(None)
                }
            }
            Err(_) => Ok(None),
        }
    }

    #[instrument(skip(self))]
    async fn get_blob_from_alt_key(
        &self,
        alt_key: &str,
        data_type: DataType,
    ) -> Result<Vec<u8>, anyhow::Error> {
        self.read_blob(
            self.get_blob_id(alt_key, data_type)
                .await?
                .ok_or(anyhow!("blob was deleted"))?,
        )
            .await
    }

    #[instrument(skip(self, form), ret)]
    pub async fn forms_edit(
        &self,
        template: String,
        form: Form,
        id: String,
    ) -> Result<(), anyhow::Error> {
        let template_blob = self
            .get_blob_from_alt_key(&template, DataType::Template)
            .await?;
        let deserialized_template: FormTemplate = serde_json::from_slice(template_blob.as_slice())?;

        if !deserialized_template.validate_form(&form) {
            return Err(anyhow!("Form does not follow template"));
        }

        if self.get_blob_id(&id, DataType::Form).await?.is_none() {
            return Err(anyhow!("Form does not exist"));
        }

        let uuid = Uuid::parse_str(&id)?;
        let mut form = form;
        form.id = Some(uuid);
        let blob_id = self.write_blob(serde_json::to_string(&form)?).await?;
        let db_form = form.to_db_form(blob_id, template);
        let transaction = Transaction::new(DataType::Form, Action::Edit, blob_id, id);

        self.write_form(db_form).await?;
        self.write_transaction(transaction).await?;

        Ok(())
    }

    #[instrument(skip(self), err)]
    pub async fn forms_delete(&self, _: String, id: String) -> Result<(), anyhow::Error> {
        let blob_id = self
            .latest_blob_from_alt_key(&id.to_string(), DataType::Form)
            .await?;

        if self.blob_deleted(blob_id).await? {
            return Ok(());
        }

        if self.get_blob_id(&id, DataType::Form).await?.is_none() {
            return Err(anyhow!("Form does not exist"));
        }

        let transaction = Transaction::new(DataType::Form, Action::Delete, blob_id, id);

        self.remove_form(blob_id).await?;
        self.write_transaction(transaction).await?;

        Ok(())
    }

    pub fn get_path(&self) -> &str {
        &self.path
    }

    #[instrument(skip(self))]
    pub async fn forms_get_serialized(
        &self,
        _: String,
        id: String,
    ) -> Result<Vec<u8>, anyhow::Error> {
        let blob_id = self.latest_blob_from_alt_key(&id, DataType::Form).await?;

        if !self.blob_exists(blob_id).await? || self.blob_deleted(blob_id).await? {
            return Err(anyhow!("Form does not exist"));
        }

        self.read_blob(blob_id).await
    }

    #[instrument(skip(self), err)]
    pub async fn forms_list(&self, template: String) -> Result<Vec<Uuid>, anyhow::Error> {
        let mut query_builder: QueryBuilder<Sqlite> = QueryBuilder::new(format!(
            "SELECT {TRANSACTION_TABLE}.alt_key AS alt_key FROM {TRANSACTION_TABLE} \
                INNER JOIN {FORMS_TABLE} ON {TRANSACTION_TABLE}.blob_id = {FORMS_TABLE}.blob_id \
                WHERE {FORMS_TABLE}.template = ? AND NOT {TRANSACTION_TABLE}.action = 'Delete'"
        ));

        let query = query_builder.build()
            .bind(template);

        let ids: Vec<Uuid> = query
            .fetch_all(&self.pool)
            .await?
            .iter()
            .filter_map(|row| {
                let str_value = row.try_get("alt_key").unwrap();
                Uuid::parse_str(str_value).ok()
            })
            .collect();

        Ok(ids)
    }

    #[instrument(skip(self), err)]
    pub async fn forms_filter(
        &self,
        template: String,
        filter: Filter,
    ) -> Result<Vec<Form>, anyhow::Error> {
        let mut query_builder: QueryBuilder<Sqlite> = QueryBuilder::new(format!(
            "SELECT blob_id FROM {FORMS_TABLE} WHERE template = "
        ));

        query_builder.push_bind(template);

        if let Some(team) = filter.team {
            query_builder.push(" AND team = ");
            query_builder.push_bind(team);
        }

        if let Some(scouter) = filter.scouter {
            query_builder.push(" AND scouter = ");
            query_builder.push_bind(scouter);
        }

        if let Some(event) = filter.event {
            query_builder.push(" AND event = ");
            query_builder.push_bind(event);
        }

        if let Some(match_number) = filter.match_number {
            query_builder.push(" AND match_number = ");
            query_builder.push_bind(match_number);
        }

        let res_ids: Vec<Uuid> = query_builder
            .build()
            .fetch_all(&self.pool)
            .await?
            .iter()
            .filter_map(|r| r.try_get("blob_id").ok())
            .collect();

        let mut res_forms: Vec<Form> = Vec::new();

        for id in res_ids {
            let ser = self.read_blob(id).await?;
            let de = serde_json::from_slice(ser.as_slice())?;

            res_forms.push(de);
        }

        Ok(res_forms)
    }

    #[instrument(skip(self, storable), err)]
    pub async fn storable_add(
        &self,
        storable: impl StorableObject,
    ) -> Result<String, anyhow::Error> {
        let check_res = self
            .get_blob_id(&storable.get_alt_key(), storable.get_type())
            .await?;

        let alt_key = storable.get_alt_key();
        let data_type = storable.get_type();

        if check_res.is_some() {
            return Err(anyhow!("Object exists already"));
        }

        let blob_id = self.write_blob(storable.ser()?).await?;
        let transaction = Transaction::new(data_type, Action::Add, blob_id, alt_key.clone());

        self.write_transaction(transaction).await?;

        Ok(alt_key)
    }

    #[instrument(skip(self, storable), err)]
    pub async fn storable_edit(&self, storable: impl StorableObject) -> Result<(), anyhow::Error> {
        let check_res = self
            .get_blob_id(&storable.get_alt_key(), storable.get_type())
            .await?;
        let alt_key = storable.get_alt_key();
        let data_type = storable.get_type();

        if check_res.is_none() {
            return Err(anyhow!("Object does not exist"));
        }

        let blob_id = self.write_blob(storable.ser()?).await?;
        let transaction = Transaction::new(data_type, Action::Edit, blob_id, alt_key.clone());

        self.write_transaction(transaction).await?;

        Ok(())
    }

    #[instrument(skip(self), err)]
    pub async fn storable_delete(
        &self,
        key: &str,
        data_type: DataType,
    ) -> Result<(), anyhow::Error> {
        let check_res = self.get_blob_id(key, data_type).await?;

        match check_res {
            None => Err(anyhow!("Object does not exist")),
            Some(id) => {
                let transaction = Transaction::new(data_type, Action::Delete, id, key.to_string());

                self.write_transaction(transaction).await?;

                Ok(())
            }
        }
    }

    #[instrument[skip(self), err]]
    pub async fn storable_list(&self, data_type: DataType) -> Result<Vec<String>, anyhow::Error> {
        let mut query_builder: QueryBuilder<Sqlite> = QueryBuilder::new(format!(
            "SELECT action, alt_key, MAX(timestamp) \
                FROM {TRANSACTION_TABLE} \
                WHERE data_type = ? \
                GROUP BY alt_key"
        ));

        let query = query_builder.build().bind(data_type);

        let out: Vec<String> = query
            .fetch_all(&self.pool)
            .await?
            .iter()
            .filter_map(|row| {
                match (row.try_get("action").ok(), row.try_get("alt_key").ok()) {
                    //i love pattern matching
                    (Some(Action::Edit | Action::Add), Some(key)) => Some(key),
                    _ => None,
                }
            })
            .collect();

        Ok(out)
    }

    #[instrument(skip(self), err)]
    pub async fn storable_get_serialized(
        &self,
        key: String,
        data_type: DataType,
    ) -> Result<Vec<u8>, anyhow::Error> {
        let mut query = QueryBuilder::new(format!(
            "SELECT blob_id, action, MAX(timestamp) \
                FROM {TRANSACTION_TABLE} \
                WHERE data_type = ? AND alt_key = ? \
                GROUP BY alt_key"
        ));
        let mut query = query.build();

        query = query.bind(data_type);
        query = query.bind(key);

        let query_res = query
            .fetch_one(&self.pool)
            .await?;

        if matches!(query_res.try_get("action")?, Action::Delete) {
            return Err(anyhow!("Object was deleted"));
        }

        let blob_id: Uuid = query_res.try_get("blob_id")?;

        self.read_blob(blob_id).await
    }
}

async fn write_non_create(
    path: impl AsRef<Path>,
    contents: impl AsRef<[u8]>,
) -> Result<(), anyhow::Error> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await?
        .write_all(contents.as_ref())
        .await
        .map_err(Into::into)
}
