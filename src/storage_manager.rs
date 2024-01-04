use crate::datatypes::FormTemplate;
use crate::transactions::{Action, DataType, InternalMessage};
use anyhow::anyhow;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::arrow::json::writer::record_batches_to_json_rows;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::{col, SessionContext};
use glob::glob;
use serde::Deserialize;
use serde_json::Value;
use sha256::Sha256Digest;
use std::path::Path;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::{fs, io};
use tracing::{info, instrument};
use uuid::Uuid;

#[derive(Default, Deserialize)]
pub struct StorageManager {
    transaction_log: TransactionLog,
    path: String,
    #[serde(skip)]
    df_ctx: SessionContext,
}

impl StorageManager {
    #[instrument(skip(self, data))]
    pub async fn raw_edit(
        &self,
        name: &str,
        old_name: &str,
        sub_path: &str,
        data: impl AsRef<[u8]>,
    ) -> Result<(), anyhow::Error> {
        info!("Edit from {sub_path}{name} to {sub_path}{old_name}");

        fs::rename(
            format!("{}{sub_path}{name}", &self.path),
            format!("{}{sub_path}{old_name}", &self.path),
        )
        .await?;

        write_non_create(format!("{}{sub_path}{name}", &self.path), data)
            .await
            .map_err(Into::into)
    }

    #[instrument(skip(self, data))]
    pub async fn raw_add(
        &self,
        name: &str,
        sub_path: &str,
        data: &[u8],
    ) -> Result<(), anyhow::Error> {
        info!("Add at {sub_path}{name}");

        write_non_create(format!("{}{sub_path}{name}", &self.path), data)
            .await
            .map_err(Into::into)
    }

    #[instrument(skip(self))]
    pub async fn raw_delete(
        &self,
        name: &str,
        old_name: &str,
        sub_path: &str,
    ) -> Result<(), anyhow::Error> {
        info!("Delete from {sub_path}{name} to {sub_path}{old_name}");

        fs::rename(
            format!("{}{sub_path}{name}", &self.path),
            format!("{}{sub_path}{old_name}", &self.path),
        )
        .await
        .map_err(Into::into)
    }

    #[instrument(skip(self))]
    pub async fn raw_get(&self, name: &str, sub_path: &str) -> Result<Vec<u8>, anyhow::Error> {
        info!("Get at {sub_path}{name}");

        fs::read(format!("{}{sub_path}{name}", &self.path))
            .await
            .map_err(Into::into)
    }

    #[instrument(skip(self))]
    async fn add_template_form_dir(&self, name: &str) -> Result<(), anyhow::Error> {
        fs::create_dir(format!("{}/forms/{name}", self.path))
            .await
            .map_err(Into::into)
    }

    #[instrument(skip(self))]
    async fn rename_template_form_dir(&self, name: &str, old: &str) -> Result<(), anyhow::Error> {
        fs::rename(
            format!("{}/forms/{name}", self.path),
            format!("{}/forms/{old}", self.path),
        )
        .await
        .map_err(Into::into)
    }

    #[instrument(skip(self))]
    async fn template_dir(&self, name: &str, old_name: Option<&str>) -> Result<(), anyhow::Error> {
        match old_name {
            None => self.add_template_form_dir(name).await,
            Some(old_name) => self.rename_template_form_dir(name, old_name).await,
        }
    }

    #[instrument(skip(self, template))]
    pub async fn templates_add(&self, template: FormTemplate) -> Result<(), anyhow::Error> {
        let digested_name = (&template.name).digest();
        let digested_name = format!("{}.current", digested_name);

        self.raw_add(
            &digested_name,
            "templates/",
            serde_json::to_string(&template).unwrap().as_bytes(),
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
            serde_json::to_string(&template).unwrap().as_bytes(),
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
            if !entry.path().to_string_lossy().contains('.') {
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

#[derive(Debug, Default, Deserialize)]
struct TransactionLog {
    path: String,
}

impl TransactionLog {
    #[instrument]
    async fn log_transaction(&self, transaction: InternalMessage) -> Result<(), anyhow::Error> {
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(&self.path)
            .await?;

        file.write_all(format!("{}\n", serde_json::to_string(&transaction)?).as_bytes())
            .await
            .map_err(Into::into)
    }

    #[instrument]
    pub async fn get_first(&self) -> Result<InternalMessage, anyhow::Error> {
        let file = File::open(&self.path).await?;
        let mut line: String = String::new();

        BufReader::new(file).read_line(&mut line).await?;

        Ok(serde_json::from_str(&line)?)
    }

    #[instrument]
    pub async fn get_after(&self, id: Uuid) -> Result<InternalMessage, anyhow::Error> {
        let file = File::open(&self.path).await?;
        let mut lines = BufReader::new(file).lines();

        while let Some(line) = lines.next_line().await? {
            let de = serde_json::from_str::<InternalMessage>(&line)?;

            if de.id == id {
                let line = lines.next_line().await?;

                return match line {
                    None => Err(anyhow!("explode")),
                    Some(line) => Ok(serde_json::from_str::<InternalMessage>(&line)?),
                };
            }
        }

        Err(anyhow!("dfasdfjkh"))
    }

    #[instrument]
    pub async fn get_file(&self, path: String) -> Result<Vec<u8>, anyhow::Error> {
        let mut buf = vec![];

        File::open(path).await?.read_to_end(&mut buf).await?;
        Ok(buf)
    }

    #[instrument]
    pub async fn list_files(&self) -> Result<Vec<String>, anyhow::Error> {
        let glob = glob("data/*")
            .unwrap()
            .filter_map(|p| p.ok())
            .filter(|p| p.is_file())
            .map(|p| p.as_path().to_string_lossy().to_string())
            .collect();

        Ok(glob)
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
