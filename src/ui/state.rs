use std::collections::BTreeMap;
use std::ops::{Deref};
use std::sync::Arc;
use std::time::Duration;
use chrono::{DateTime, Utc};
use moka::future::{Cache, CacheBuilder};
use moka::policy::EvictionPolicy;
use tokio::sync::RwLock;
use crate::transactions::DataType;

impl InterfaceManager {
    pub fn new(mem_size: u64) -> Self {
        let mut builder = CacheBuilder::new(mem_size)
            .eviction_policy(EvictionPolicy::tiny_lfu())
            .time_to_live(Duration::from_secs(15 * 60));

        Self {
            cache: builder.build()
        }
    }

    pub fn get_cache(&self) -> Cache<String, InterfaceCacheEntry> {
        self.cache.clone()
    }
}

impl Default for InterfaceState {
    fn default() -> Self {
        Self::Search
    }
}

pub struct InterfaceManager {
    cache: Cache<String, InterfaceCacheEntry>,
}

#[derive(Clone)]
pub enum InterfaceState {
    Search,
    View(String),
}


#[derive(Ord, PartialOrd, Eq, PartialEq, Clone)]
pub struct InterfaceCacheEntry {
    pub timestamp: DateTime<Utc>,
    pub entry_type: EntryType,
    pub count: i64,
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Clone)]
pub enum EntryType {
    Search(String),
    AltKeyTableEntry(String, DataType),
    AltKeyFullHistory(String),
}