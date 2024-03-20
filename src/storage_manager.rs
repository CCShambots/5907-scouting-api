use std::any::Any;
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
use sqlx::{query, Executor, QueryBuilder, Row, Sqlite, SqlitePool, Execute, ValueRef, FromRow};
use std::ops::Add;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use datafusion::sql::sqlparser::keywords::Keyword::TRANSACTION;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::{fs, io};
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;
use crate::sync::ChildID;

pub const TRANSACTION_TABLE: &str = "transactions";
pub const FORMS_TABLE: &str = "forms";
pub const WATER_MARK_TABLE: &str = "watermarks";
pub const NEEDED_BLOB_TABLE: &str = "blobs";

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

        write(format!("{}{}", self.path, id.to_string().digest()), data).await?;

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
            sqlx::query(&format!(
                "SELECT action FROM {TRANSACTION_TABLE} \
                WHERE blob_id = ? AND (id, timestamp) IN (\
                    SELECT id, MAX(timestamp) FROM {TRANSACTION_TABLE} GROUP BY id\
                )"
            ))
                .bind(id)
                .fetch_one(&self.pool)
                .await?
                .try_get("action")?;

        Ok(matches!(res, Action::Delete))
    }

    #[instrument(skip(self))]
    pub async fn latest_transaction_from_alt_key(
        &self,
        alt_key: &str,
        data_type: DataType,
    ) -> Result<Transaction, anyhow::Error> {
        let res = sqlx::query(&format!(
            "SELECT * FROM {TRANSACTION_TABLE} \
            WHERE alt_key = ? AND data_type = ? AND (id, timestamp) IN (\
                SELECT id, Max(timestamp) FROM {TRANSACTION_TABLE} GROUP BY id\
            )"
        ))
            .bind(alt_key)
            .bind(data_type)
            .fetch_one(&self.pool).await?;

        Ok(Transaction::from_row(&res)?)
    }

    #[instrument(skip(self))]
    pub async fn transaction_count(
        &self,
        alt_key: &str,
        data_type: DataType,
    ) -> Result<i64, anyhow::Error> {
        let res: i64 = sqlx::query(&format!(
            "SELECT COUNT(*) as count FROM {TRANSACTION_TABLE} \
            WHERE alt_key = ? AND data_type = ?"
        ))
            .bind(alt_key)
            .bind(data_type)
            .fetch_one(&self.pool).await?
            .try_get("count")?;

        Ok(res)
    }

    #[instrument(skip(self))]
    pub async fn latest_blob_from_alt_key(
        &self,
        alt_key: &str,
        data_type: DataType,
    ) -> Result<Uuid, anyhow::Error> {
        let res: Uuid = sqlx::query(&format!(
            "SELECT blob_id FROM {TRANSACTION_TABLE} \
            WHERE alt_key = ? AND data_type = ? AND (id, timestamp) IN (\
                SELECT id, MAX(timestamp) FROM {TRANSACTION_TABLE} GROUP BY id\
            )"
        ))
            .bind(alt_key)
            .bind(data_type)
            .fetch_one(&self.pool).await?
            .try_get("blob_id")?;

        Ok(res)
    }

    #[instrument(skip(self))]
    pub async fn latest_action_from_alt_key(
        &self,
        alt_key: &str,
        data_type: DataType,
    ) -> Result<Action, anyhow::Error> {
        let res: Action = sqlx::query(&format!(
            "SELECT action FROM {TRANSACTION_TABLE} \
            WHERE alt_key = ? AND data_type = ? AND (id, timestamp) IN (\
                SELECT id, MAX(timestamp) FROM {TRANSACTION_TABLE} GROUP BY id\
            )"
        ))
            .bind(alt_key)
            .bind(data_type)
            .fetch_one(&self.pool).await?
            .try_get("action")?;

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

    #[instrument(skip(self, form), err)]
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

        //self.remove_form(blob_id).await?;
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
        /*let mut query_builder: QueryBuilder<Sqlite> = QueryBuilder::new(format!(
            "SELECT {TRANSACTION_TABLE}.alt_key AS alt_key, MAX({TRANSACTION_TABLE}.timestamp) FROM {TRANSACTION_TABLE} \
                INNER JOIN {FORMS_TABLE} ON {TRANSACTION_TABLE}.blob_id = {FORMS_TABLE}.blob_id \
                WHERE {FORMS_TABLE}.template = ? AND NOT {TRANSACTION_TABLE}.action = 'Delete' \
                GROUP BY alt_key"
        ));*/

        //subqueries :()
        let mut query_builder: QueryBuilder<Sqlite> = QueryBuilder::new(format!(
            "SELECT alt_key, LAST_VALUE(action) OVER (ORDER BY timestamp) as last_action, MAX(timestamp) \
                FROM (
         SELECT alt_key, LAST_VALUE(action) OVER (ORDER BY timestamp) as last_action, MAX(timestamp)
         FROM {TRANSACTION_TABLE} \
         WHERE data_type = 'Form' \
         GROUP BY alt_key \
     ) \
                WHERE data_type = 'Form' AND last_action IS NOT 'Delete' \
                GROUP BY alt_key"
        ));

        let query = query_builder.build()
            .bind(template);

        let ids: Vec<Uuid> = query
            .fetch_all(&self.pool)
            .await?
            .iter()
            .filter_map(|row| {
                match (row.try_get("action"), row.try_get::<String, _>("alt_key")) {
                    //i love pattern matching
                    (Ok(Action::Edit | Action::Add), Ok(key)) =>
                        Uuid::parse_str(&key).ok(),
                    _ => None,
                }
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
            "SELECT blob_id \
FROM ( \
         SELECT blob_id, LAST_VALUE(action) OVER (ORDER BY timestamp) as last_action, MAX(timestamp) \
         FROM {TRANSACTION_TABLE} \
         WHERE data_type = 'Form' \
         GROUP BY alt_key
     ) \
WHERE last_action IS NOT 'Delete' AND blob_id IN ( \
        SELECT blob_id \
        FROM {FORMS_TABLE} \
        WHERE template = "
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

        query_builder.push(")");

        let res_ids: Vec<Uuid> = query_builder
            .build()
            .fetch_all(&self.pool)
            .await?
            .iter()
            .filter_map(|row| {
                row.try_get("blob_id").ok()
            })
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

    #[instrument(skip(self))]
    pub async fn search_count(&self, search: &str) -> Result<i64, anyhow::Error> {
        let count = sqlx::query::<Sqlite>(&format!(
            "SELECT COUNT(*) as count FROM (\
                SELECT DISTINCT alt_key \
                FROM {TRANSACTION_TABLE} \
                WHERE alt_key = %?%\
            )"
        ))
            .bind(search)
            .fetch_one(&self.pool)
            .await?
            .get("count");

        Ok(count)
    }

    pub async fn search(&self, search: &str) -> Result<Vec<Transaction>, anyhow::Error> {
        let res: Vec<Transaction> = sqlx::query::<Sqlite>(&format!(
            "SELECT * FROM {TRANSACTION_TABLE} \
            WHERE alt_key = %?% AND (alt_key, timestamp) IN (\
                SELECT alt_key, MAX(timestamp) as timestamp FROM {TRANSACTION_TABLE} \
                GROUP BY alt_key\
            )"
        ))
            .bind(search)
            .fetch_all(&self.pool)
            .await?
            .iter()
            .filter_map(|row| Transaction::from_row(row).ok())
            .collect();

        Ok(res)
    }

    #[instrument[skip(self), err]]
    pub async fn storable_list(&self, data_type: DataType) -> Result<Vec<String>, anyhow::Error> {
        let mut query_builder: QueryBuilder<Sqlite> = QueryBuilder::new(format!(
            "SELECT alt_key, action, id, timestamp \
                FROM {TRANSACTION_TABLE} \
                WHERE data_type = ? AND (id, timestamp) IN (\
                    SELECT id, MAX(timestamp) FROM {TRANSACTION_TABLE} GROUP BY id\
                )"
        ));

        let query = query_builder.build().bind(data_type);

        let out: Vec<String> = query
            .fetch_all(&self.pool)
            .await?
            .iter()
            .filter_map(|row| {
                match (row.try_get("action"), row.try_get("alt_key")) {
                    //i love pattern matching
                    (Ok(Action::Edit | Action::Add), Ok(key)) => Some(key),
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
            "SELECT blob_id, action, id, timestamp \
                FROM {TRANSACTION_TABLE} \
                WHERE data_type = ? AND alt_key = ? AND (id, timestamp) IN (\
                    SELECT id, MAX(timestamp) FROM {TRANSACTION_TABLE} GROUP BY id\
                )"
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

    pub async fn update_watermark(&self, owner_id: Uuid, transaction_id: Uuid) -> Result<(), anyhow::Error> {
        let mut transaction = self.pool.begin().await?;

        sqlx::query(&format!(
            "DELETE FROM {WATER_MARK_TABLE} \
            WHERE owner_id = ?"
        ))
            .bind(owner_id)
            .execute(transaction.as_mut())
            .await?;

        sqlx::query(&format!(
            "INSERT INTO {WATER_MARK_TABLE} \
            (owner_id, transaction_id), VALUES(?, ?)"
        ))
            .bind(owner_id)
            .bind(transaction_id)
            .execute(transaction.as_mut())
            .await?;

        transaction.commit().await?;

        Ok(())
    }

    pub async fn get_needed_blobs(&self, owner_id: Uuid) -> Result<Vec<Uuid>, anyhow::Error> {
        let res: Vec<Uuid> = sqlx::query(&format!(
            "SELECT * FROM {NEEDED_BLOB_TABLE} \
            WHERE owner_id = ?"
        ))
            .bind(owner_id)
            .fetch_all(&self.pool)
            .await?
            .iter()
            .filter_map(|r| r.try_get("blob_id").ok())
            .collect();

        Ok(res)
    }

    pub async fn write_foreign_transaction(&self, transaction: Transaction) -> Result<(), anyhow::Error> {
        let mut transaction = transaction;
        transaction.timestamp = Utc::now().timestamp_micros();
        self.write_transaction(transaction).await?;

        Ok(())
    }

    pub async fn get_transaction(&self, id: Uuid) -> Result<Transaction, anyhow::Error> {
        let mut query: QueryBuilder<Sqlite> = QueryBuilder::new(format!(
            "SELECT * \
            FROM {TRANSACTION_TABLE} \
            WHERE id = "
        ));

        query.push_bind(id);

        let transaction: Transaction = query
            .build_query_as()
            .fetch_one(&self.pool)
            .await?;

        Ok(transaction)
    }

    pub async fn get_transaction_after(&self, id: Uuid) -> Result<Option<Transaction>, anyhow::Error> {
        let transaction = self.get_transaction(id).await?;

        let res = sqlx::query::<Sqlite>(&format!(
            "SELECT * FROM {TRANSACTION_TABLE}\
            WHERE (id, timestamp) IN (\
                SELECT id, MIN(timestamp) as timestamp \
                FROM {TRANSACTION_TABLE} \
                WHERE timestamp > ? \
                GROUP BY id\
            )"
        ))
            .bind(transaction.timestamp)
            .fetch_optional(&self.pool)
            .await?
            .map(|row| Transaction::from_row(&row).unwrap());

        Ok(res)
    }

    pub async fn restore_transaction(&self, id: Uuid) -> Result<(), anyhow::Error> {
        let mut transaction = self.get_transaction(id).await?;
        transaction.timestamp = Utc::now().timestamp_micros();
        transaction.id = Uuid::new_v4();

        let last_action = self.latest_action_from_alt_key(&transaction.alt_key, transaction.data_type).await?;

        transaction.action = match last_action {
            Action::Add | Action::Edit => Action::Edit,
            Action::Delete => Action::Add
        };

        self.write_transaction(transaction).await?;

        Ok(())
    }
}

async fn write(
    path: impl AsRef<Path>,
    contents: impl AsRef<[u8]>,
) -> Result<(), anyhow::Error> {
    OpenOptions::new()
        .write(true)
        .create(true)
        .open(path)
        .await?
        .write_all(contents.as_ref())
        .await
        .map_err(Into::into)
}

pub struct Watermark {
    owner_id: Uuid,
    transaction_id: Uuid,
}
