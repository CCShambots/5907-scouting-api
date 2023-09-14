use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;
use crate::data::db_layer::DBLayer;

impl SyncHandler {
    fn new(db_layer: Arc<DBLayer>) -> Self {
        Self {
            children: HashMap::new(),
            db_layer
        }
    }

    fn add_child(&mut self, id: Uuid) -> Result<(), SyncError> {
        match self.children.contains_key(&id) {
            true => Err(SyncError::ChildExists(id)),
            false => {
                self.children.insert(id, None);
                Ok(())
            }
        }
    }
}

struct SyncHandler {
    children: HashMap<Uuid, Option<Uuid>>,
    db_layer: Arc<DBLayer>
}

enum SyncError {
    ChildExists(Uuid)
}