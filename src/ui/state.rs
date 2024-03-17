use std::collections::BTreeMap;
use std::ops::{Deref};
use std::sync::Arc;
use chrono::{DateTime, Utc};
use tokio::sync::RwLock;

impl InterfaceManager {
    pub async fn get_state(&self) -> InterfaceState {
        self.state.read().await.deref().clone()
    }

    pub async fn set_state(&self, state: InterfaceState) {
        let mut inner_state = self.state.write().await;
        *inner_state = state;
    }

    pub async fn check_cache(&self, id: &str) -> Option<InterfaceCacheEntry> {
        self.cache.read().await.get(id).cloned()
    }

    pub async fn set_cache(&self, key: String, entry_type: EntryType, count: i64) {
        self.cache.write().await.insert(
            key,
            InterfaceCacheEntry {
                timestamp: Utc::now(),
                entry_type,
                count,
            },
        );
    }

    pub async fn clear_cache_entry(&self, id: &str) {
        self.cache.write().await.remove(id);
    }

    pub async fn invalidate_cache(&self) {
        self.cache.write().await.clear();
    }
}

impl Default for InterfaceState {
    fn default() -> Self {
        Self::Search
    }
}

#[derive(Default)]
pub struct InterfaceManager {
    state: Arc<RwLock<InterfaceState>>,
    cache: Arc<RwLock<BTreeMap<String, InterfaceCacheEntry>>>,
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
    AltKeyTableEntry(String),
    AltKeyFullHistory(String),
}