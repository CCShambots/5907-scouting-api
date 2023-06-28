use std::fs::OpenOptions;
use std::io::Write;
use config::{Config, ConfigError, File};
use serde::{Deserialize, Serialize};


impl Settings {
    pub fn new(path: &str) -> Result<Self, ConfigError> {
        let serialized = Config::builder()
            .add_source(File::with_name(path))
            .build();

        match serialized {
            Ok(res) => {
                res.try_deserialize()
            }
            Err(_) => {
                println!(
                    "There was an error reading the configuration file, or the configuration file was not found. Reverting to default configuration and overwriting previous configuration file."
                );

                let mut file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(path)
                    .unwrap();

                let default = Settings::default();

                let pretty_default =
                    toml::ser::to_string_pretty(&default)
                        .unwrap();

                file.write_all(pretty_default.as_bytes()).unwrap();

                Ok(default)
            }
        }
    }
}

impl Default for Database {
    fn default() -> Self {
        Self {
            cache_capacity: 1024 * 1024 * 1024,
            path: "database".to_owned()
        }
    }
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            template_directory: "templates".to_owned(),
            parent_address: None,
            database: Database::default()
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Database {
    pub cache_capacity: u64,
    pub path: String
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Settings {
    pub template_directory: String,
    pub parent_address: Option<String>,
    pub database: Database
}