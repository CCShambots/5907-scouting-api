use crate::datatypes::FormTemplate;
use crate::transactions::{AddType, EditType, Internal, InternalMessage, RemoveType};
use anyhow::anyhow;
use glob::glob;
use serde::Deserialize;
use std::path::Path;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::{fs, io};
use tracing::{info, instrument};
use uuid::Uuid;

#[derive(Debug, Default, Deserialize)]
pub struct StorageManager {
    transaction_log: TransactionLog,
    path: String,
}

impl StorageManager {
    #[instrument(skip(data))]
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

    #[instrument(skip(data))]
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

    #[instrument]
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

    #[instrument]
    pub async fn raw_get(&self, name: &str, sub_path: &str) -> Result<Vec<u8>, anyhow::Error> {
        info!("Get at {sub_path}{name}");

        fs::read(format!("{}{sub_path}{name}", &self.path))
            .await
            .map_err(Into::into)
    }

    #[instrument(skip(data))]
    pub async fn bytes_add(
        &self,
        name: String,
        desired_key: String,
        data: &[u8],
    ) -> Result<(), anyhow::Error> {
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
            .log_transaction(InternalMessage::new(Internal::Add(AddType::Bytes(name))))
            .await
            .map_err(Into::into)
    }

    #[instrument(skip(data))]
    pub async fn bytes_edit(
        &self,
        name: String,
        desired_key: String,
        data: &[u8],
    ) -> Result<(), anyhow::Error> {
        let old = format!("{}.{}", &name, Uuid::new_v4());

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
            .log_transaction(InternalMessage::new(Internal::Edit(EditType::Bytes(
                name, old,
            ))))
            .await
            .map_err(Into::into)
    }

    #[instrument]
    pub async fn bytes_delete(&self, name: String) -> Result<(), anyhow::Error> {
        let old = format!("{}.{}", &name, Uuid::new_v4());

        self.raw_delete(&name, &old, "bytes/").await?;

        self.transaction_log
            .log_transaction(InternalMessage::new(Internal::Remove(RemoveType::Bytes(
                name, old,
            ))))
            .await
            .map_err(Into::into)
    }

    #[instrument]
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

    #[instrument]
    pub async fn bytes_get(&self, name: String) -> Result<Vec<u8>, anyhow::Error> {
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
