use std::ops::Add;
use crate::datatypes::{DBForm, Filter, Form, FormTemplate, Schedule};
use crate::transactions::{Action, DataType, InternalMessage, Transaction};
use anyhow::anyhow;
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
use datafusion::prelude::{col, DataFrame, lit, max, NdJsonReadOptions, SessionContext};
use glob::glob;
use serde::Deserialize;
use serde_json::Value;
use sha256::Sha256Digest;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use chrono::Utc;
use sqlx::{Executor, QueryBuilder, Row, Sqlite, SqlitePool};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::{fs, io};
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

const TRANSACTION_TABLE: &str = &"transactions";
const FORMS_TABLE: &str = &"forms";

pub struct StorageManager {
    path: String,
    pool: SqlitePool,
}

impl StorageManager {
    #[instrument(ret, skip(self, data))]
    async fn write_blob(&self, data: impl AsRef<[u8]>) -> Result<Uuid, anyhow::Error> {
        let id: Uuid = Uuid::new_v4();

        write_non_create(format!("{}{}", self.path, id.to_string().digest()), data).await?;

        Ok(id)
    }

    #[instrument(skip(self))]
    async fn read_blob(&self, id: Uuid) -> Result<Vec<u8>, anyhow::Error> {
        fs::read(format!("{}{}", self.path, id.to_string().digest())).await.map_err(Into::into)
    }

    #[instrument(skip(self, form))]
    async fn write_form(&self, form: DBForm) -> Result<(), anyhow::Error> {
        let mut query_builder: QueryBuilder<Sqlite> = QueryBuilder::new(
            format!("INSERT INTO {FORMS_TABLE} (blob_id, team, match_number, event_key, template)")
        );

        query_builder.push_bind(form.blob_id);
        query_builder.push_bind(form.team);
        query_builder.push_bind(form.match_number);
        query_builder.push_bind(form.event_key);
        query_builder.push_bind(form.template);

        if self.check_form_exists(form.blob_id).await? {
            self.remove_form(form.blob_id).await?;
        }

        self.pool.execute(query_builder.build()).await?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn check_form_exists(&self, blob_id: Uuid) -> Result<bool, anyhow::Error> {
        let mut query_builder: QueryBuilder<Sqlite> = QueryBuilder::new(
            format!("SELECT COUNT(*) AS count FROM {FORMS_TABLE} WHERE blob_id = ?")
        );

        query_builder.push_bind(blob_id);

        let count: i64 = query_builder.build()
            .fetch_one(&self.pool).await?
            .try_get("count")?;

        Ok(count > 0)
    }

    #[instrument(skip(self))]
    async fn remove_form(&self, blob_id: Uuid) -> Result<(), anyhow::Error> {
        let mut query_builder: QueryBuilder<Sqlite> = QueryBuilder::new(
            format!("DELETE FROM {FORMS_TABLE} WHERE blob_id = ?")
        );

        query_builder.push_bind(blob_id);

        self.pool.execute(query_builder.build()).await?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn write_transaction(&self, transaction: Transaction) -> Result<(), anyhow::Error> {
        let mut query_builder: QueryBuilder<Sqlite> = QueryBuilder::new(
            format!("INSERT INTO {TRANSACTION_TABLE} (id, data_type, action, blob_id, timestamp)")
        );

        query_builder.push_bind(transaction.id);
        query_builder.push_bind(transaction.data_type);
        query_builder.push_bind(transaction.action);
        query_builder.push_bind(transaction.blob_id);
        query_builder.push_bind(transaction.timestamp);

        self.pool.execute(query_builder.build()).await?;

        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn blob_exists(&self, id: Uuid) -> Result<bool, anyhow::Error> {
        fs::try_exists(format!("{}{}", self.path, id.to_string().digest())).await.map_err(Into::into)
    }

    #[instrument(skip(self))]
    pub async fn blob_deleted(&self, id: Uuid) -> Result<bool, anyhow::Error> {
        let res: Action = sqlx::query("SELECT action FROM transactions WHERE id = ? ORDER BY timestamp DESC")
            .bind(id)
            .fetch_one(&self.pool).await?
            .try_get("action")?;

        Ok(matches!(res, Action::Delete))
    }

    #[instrument(skip(self))]
    pub async fn latest_blob_from_alt_key(&self, alt_key: &str, data_type: DataType) -> Result<Uuid, anyhow::Error> {
        let res: Uuid = sqlx::query("SELECT id FROM transactions WHERE alt_key = ? AND data_type = ? ORDER BY timestamp DESC")
            .bind(alt_key)
            .bind(data_type)
            .fetch_one(&self.pool).await?
            .try_get("id")?;

        Ok(res)
    }

    #[instrument(skip(self, form))]
    pub async fn forms_add(&self, template: String, form: Form) -> Result<Uuid, anyhow::Error> {
        let blob_id = self.write_blob(serde_json::to_string(&form)?).await?;
        let db_form = form.to_db_form(blob_id, template);
        let transaction = Transaction::new(
            DataType::Form,
            Action::Add,
            blob_id,
            blob_id.to_string(),
        );

        self.write_form(db_form).await?;
        self.write_transaction(transaction).await?;

        Ok(blob_id)
    }

    #[instrument(skip(self))]
    async fn check_alt_key_exists(&self, alt_key: &str, data_type: DataType) -> Result<(bool, Uuid), anyhow::Error> {
        let blob_id = self.latest_blob_from_alt_key(&alt_key, data_type).await?;
        Ok((self.blob_exists(blob_id).await? && !self.blob_deleted(blob_id).await?, blob_id))
    }

    #[instrument(skip(self))]
    async fn get_blob_from_alt_key(&self, alt_key: &str, data_type: DataType) -> Result<Vec<u8>, anyhow::Error> {
        let check_result = self.check_alt_key_exists(alt_key, data_type).await?;

        if !check_result.0 {
            return Err(anyhow!("Does not exist"));
        }

        self.read_blob(check_result.1).await
    }

    #[instrument(skip(self, form), ret)]
    pub async fn forms_edit(
        &self,
        template: String,
        form: Form,
        id: Uuid,
    ) -> Result<(), anyhow::Error> {
        let template_blob = self.get_blob_from_alt_key(&template, DataType::Template).await?;
        let deserialized_template: FormTemplate = serde_json::from_slice(template_blob.as_slice())?;

        if !deserialized_template.validate_form(&form) {
            return Err(anyhow!("Form does not follow template"));
        }

        let blob_id = self.write_blob(serde_json::to_string(&form)?).await?;
        let db_form = form.to_db_form(blob_id, template);
        let transaction = Transaction::new(
            DataType::Form,
            Action::Edit,
            blob_id,
            id.to_string(),
        );

        self.write_form(db_form).await?;
        self.write_transaction(transaction).await?;

        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn forms_delete(&self, _: String, id: Uuid) -> Result<(), anyhow::Error> {
        let blob_id = self.latest_blob_from_alt_key(&id.to_string(), DataType::Form).await?;

        if self.blob_deleted(blob_id).await? {
            return Ok(());
        }

        let transaction = Transaction::new(
            DataType::Form,
            Action::Delete,
            blob_id,
            id.to_string(),
        );

        self.remove_form(blob_id).await?;
        self.write_transaction(transaction).await?;

        Ok(())
    }

    pub fn get_path(&self) -> &str {
        &self.path
    }

    #[instrument(skip(self))]
    pub async fn forms_get_serialized(&self, _: String, id: String) -> Result<Vec<u8>, anyhow::Error> {
        let blob_id = self.latest_blob_from_alt_key(&id, DataType::Form).await?;

        if !self.blob_exists(blob_id).await? || self.blob_deleted(blob_id).await? {
            return Err(anyhow!("Form does not exist"));
        }

        self.read_blob(blob_id).await
    }

    #[instrument(skip(self))]
    pub async fn forms_list(&self, template: String) -> Result<Vec<Uuid>, anyhow::Error> {
        let mut query_builder: QueryBuilder<Sqlite> = QueryBuilder::new(
            format!("SELECT blob_id FROM {FORMS_TABLE} WHERE template = ?")
        );

        query_builder.push_bind(template);

        let blob_ids: Vec<Uuid> = query_builder.build()
            .fetch_all(&self.pool).await?
            .iter()
            .filter_map(|row| row.try_get("blob_id").ok())
            .collect();

        Ok(blob_ids)
    }

    #[instrument(skip(self))]
    pub async fn forms_filter(
        &self,
        template: String,
        filter: Filter,
    ) -> Result<Vec<Form>, anyhow::Error> {
        let mut query_builder: QueryBuilder<Sqlite> = QueryBuilder::new(
            format!("SELECT blob_id FROM {FORMS_TABLE} WHERE template = ?")
        );

        if let Some(team) = filter.team {
            query_builder.push(" AND team = ?");
            query_builder.push_bind(team);
        }

        if let Some(scouter) = filter.scouter {
            query_builder.push(" AND scouter = ?");
            query_builder.push_bind(scouter);
        }

        if let Some(event) = filter.event {
            query_builder.push(" AND event = ?");
            query_builder.push_bind(event);
        }

        if let Some(match_number) = filter.match_number {
            query_builder.push(" AND match_number = ?");
            query_builder.push_bind(match_number);
        }

        todo!()
    }

    #[instrument(skip(self, schedule))]
    pub async fn schedules_add(&self, schedule: Schedule) -> Result<(), anyhow::Error> {
        let digested_name = (&schedule.event).digest();
        let digested_name = format!("{}.current", digested_name);

        self.raw_add(
            &digested_name,
            "schedules/",
            serde_json::to_string(&schedule)?.as_bytes(),
        )
            .await?;

        self.transaction_log
            .log_transaction(InternalMessage::new(
                DataType::Schedule,
                Action::Add,
                digested_name,
            ))
            .await
    }

    #[instrument(skip(self, schedule))]
    pub async fn schedules_edit(&self, schedule: Schedule) -> Result<(), anyhow::Error> {
        let digested_name = (&schedule.event).digest();
        let old = format!("{}.{}", &digested_name, Uuid::new_v4());
        let digested_name = format!("{}.current", digested_name);

        self.raw_edit(
            &digested_name,
            &old,
            "schedules/",
            serde_json::to_string(&schedule)?.as_bytes(),
        )
            .await?;

        self.transaction_log
            .log_transaction(InternalMessage::new(DataType::Schedule, Action::Edit, old))
            .await
    }

    #[instrument(skip(self))]
    pub async fn schedules_delete(&self, name: String) -> Result<(), anyhow::Error> {
        let digested_name = (&name).digest();
        let old = format!("{}.{}", &digested_name, Uuid::new_v4());
        let digested_name = format!("{}.current", digested_name);

        self.raw_delete(&digested_name, &old, "schedules/").await?;

        self.transaction_log
            .log_transaction(InternalMessage::new(
                DataType::Schedule,
                Action::Delete,
                old,
            ))
            .await
    }

    #[instrument(skip(self))]
    pub async fn schedules_get(&self, name: String) -> Result<Schedule, anyhow::Error> {
        let digested_name = (&name).digest();
        let digested_name = format!("{}.current", digested_name);

        let bytes = self.raw_get(&digested_name, "schedules/").await?;

        serde_json::from_slice(bytes.as_slice()).map_err(Into::into)
    }

    #[instrument(skip(self))]
    pub async fn schedules_list(&self) -> Result<Vec<String>, anyhow::Error> {
        if !self.df_ctx.table_exist("schedules")? {
            let path = ListingTableUrl::parse(format!("{}schedules", self.path))?;
            let file_format = JsonFormat::default();
            let listing_options =
                ListingOptions::new(Arc::new(file_format)).with_file_extension(".current");
            let schema = SchemaRef::new(Schema::new(vec![Field::new(
                "event",
                datafusion::arrow::datatypes::DataType::Utf8,
                false,
            )]));
            let config = ListingTableConfig::new(path)
                .with_listing_options(listing_options)
                .with_schema(schema);
            let provider = Arc::new(ListingTable::try_new(config)?);

            self.df_ctx.register_table("schedules", provider)?;
        }

        let df = self.df_ctx.table("schedules").await?;
        let res = df.select(vec![col("event")])?.collect().await?;

        let res: Vec<&RecordBatch> = res.iter().collect();

        let res = record_batches_to_json_rows(res.as_slice())?;

        let res = res
            .iter()
            .filter_map(|m| m.get("event"))
            .filter_map(|thing| match thing {
                Value::String(s) => Some(s.clone()),
                _ => None,
            })
            .collect();

        Ok(res)
    }

    #[instrument(skip(self, template))]
    pub async fn templates_add(&self, template: FormTemplate) -> Result<(), anyhow::Error> {
        let digested_name = (&template.name).digest();
        let digested_name = format!("{}.current", digested_name);

        self.raw_add(
            &digested_name,
            "templates/",
            serde_json::to_string(&template)?.as_bytes(),
        )
            .await?;

        self.template_dir(&digested_name, None).await?;

        self.transaction_log
            .log_transaction(InternalMessage::new(
                DataType::Template,
                Action::Add,
                digested_name,
            ))
            .await
    }

    #[instrument(skip(self, template))]
    pub async fn templates_edit(&self, template: FormTemplate) -> Result<(), anyhow::Error> {
        let digested_name = (&template.name).digest();
        let old = format!("{}.{}", &digested_name, Uuid::new_v4());
        let digested_name = format!("{}.current", digested_name);

        self.raw_edit(
            &digested_name,
            &old,
            "templates/",
            serde_json::to_string(&template)?.as_bytes(),
        )
            .await?;

        self.template_dir(&digested_name, Some(&old)).await?;
        self.template_dir(&digested_name, None).await?;

        self.transaction_log
            .log_transaction(InternalMessage::new(DataType::Template, Action::Edit, old))
            .await
    }

    #[instrument(skip(self))]
    pub async fn templates_delete(&self, name: String) -> Result<(), anyhow::Error> {
        let digested_name = name.digest();
        let old = format!("{}.{}", &digested_name, Uuid::new_v4());
        let digested_name = format!("{}.current", digested_name);

        self.raw_delete(&digested_name, &old, "templates/").await?;

        self.template_dir(&digested_name, Some(&old)).await?;

        self.transaction_log
            .log_transaction(InternalMessage::new(
                DataType::Template,
                Action::Delete,
                old,
            ))
            .await
    }

    #[instrument(skip(self))]
    pub async fn templates_get(&self, name: String) -> Result<FormTemplate, anyhow::Error> {
        let digested_name = name.digest();
        let digested_name = format!("{}.current", digested_name);
        let bytes = self.raw_get(&digested_name, "templates/").await?;

        serde_json::from_slice(bytes.as_slice()).map_err(Into::into)
    }

    #[instrument(skip(self), ret)]
    pub async fn templates_list(&self) -> Result<Vec<String>, anyhow::Error> {
        if !self.df_ctx.table_exist("templates")? {
            let path = ListingTableUrl::parse(format!("{}templates", self.path))?;
            let file_format = JsonFormat::default();
            let listing_options =
                ListingOptions::new(Arc::new(file_format)).with_file_extension(".current");
            let schema = SchemaRef::new(Schema::new(vec![Field::new(
                "name",
                datafusion::arrow::datatypes::DataType::Utf8,
                false,
            )]));
            let config = ListingTableConfig::new(path)
                .with_listing_options(listing_options)
                .with_schema(schema);
            let provider = Arc::new(ListingTable::try_new(config)?);

            self.df_ctx.register_table("templates", provider)?;
        }

        let df = self.df_ctx.table("templates").await?;
        let res = df.select(vec![col("name")])?.collect().await?;

        let res: Vec<&RecordBatch> = res.iter().collect();

        let res = record_batches_to_json_rows(res.as_slice())?;

        let res = res
            .iter()
            .filter_map(|m| m.get("name"))
            .filter_map(|thing| match thing {
                Value::String(s) => Some(s.clone()),
                _ => None,
            })
            .collect();

        Ok(res)
    }

    #[instrument(skip(self, data))]
    pub async fn bytes_add(
        &self,
        name: String,
        desired_key: String,
        data: &[u8],
    ) -> Result<(), anyhow::Error> {
        let name = format!("{name}.current");

        self.raw_add(
            &name,
            "bytes/",
            &[
                &(desired_key.len() as u64).to_be_bytes(),
                desired_key.as_bytes(),
                data,
            ]
                .concat(),
        )
            .await?;

        self.transaction_log
            .log_transaction(InternalMessage::new(DataType::Bytes, Action::Add, name))
            .await
    }

    #[instrument(skip(self, data))]
    pub async fn bytes_edit(
        &self,
        name: String,
        desired_key: String,
        data: &[u8],
    ) -> Result<(), anyhow::Error> {
        let old = format!("{}.{}", &name, Uuid::new_v4());
        let name = format!("{name}.current");

        self.raw_edit(
            &name,
            &old,
            "bytes/",
            &[
                &(desired_key.len() as u64).to_be_bytes(),
                desired_key.as_bytes(),
                data,
            ]
                .concat(),
        )
            .await?;

        self.transaction_log
            .log_transaction(InternalMessage::new(DataType::Bytes, Action::Add, old))
            .await
    }

    #[instrument(skip(self))]
    pub async fn bytes_delete(&self, name: String) -> Result<(), anyhow::Error> {
        let old = format!("{}.{}", &name, Uuid::new_v4());
        let name = format!("{name}.current");

        self.raw_delete(&name, &old, "bytes/").await?;

        self.transaction_log
            .log_transaction(InternalMessage::new(DataType::Bytes, Action::Add, old))
            .await
    }

    #[instrument(skip(self))]
    pub async fn bytes_list(&self) -> Result<Vec<String>, anyhow::Error> {
        let mut entries = fs::read_dir(format!("{}bytes/", self.path)).await?;
        let mut keys: Vec<String> = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            if entry.path().to_string_lossy().ends_with(".current") {
                let mut f = File::open(entry.path()).await?;
                let len = f.read_u64().await?;
                let mut bytes = vec![0_u8; len as usize];

                f.read_exact(&mut bytes).await?;

                keys.push(String::from_utf8_lossy(&bytes[..]).to_string());
            }
        }

        Ok(keys)
    }

    #[instrument(skip(self))]
    pub async fn bytes_get(&self, name: String) -> Result<Vec<u8>, anyhow::Error> {
        let name = format!("{name}.current");

        let bytes = self.raw_get(&name, "bytes/").await?;

        let len = u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);

        Ok(Vec::from(&bytes[(len as usize + 8)..]))
    }

    pub async fn get_first(&self) -> Result<InternalMessage, anyhow::Error> {
        self.transaction_log.get_first().await
    }

    pub async fn get_after(&self, id: Uuid) -> Result<InternalMessage, anyhow::Error> {
        self.transaction_log.get_after(id).await
    }

    pub async fn list_files(&self) -> Result<Vec<String>, anyhow::Error> {
        self.transaction_log.list_files().await
    }

    pub async fn get_file(&self, path: String) -> Result<Vec<u8>, anyhow::Error> {
        self.transaction_log.get_file(path).await
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
