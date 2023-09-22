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

    fn add_child(&mut self, id: Uuid) -> Result<(), Error> {
        match self.children.contains_key(&id) {
            true => Err(Error::ChildExists(id)),
            false => {
                self.children.insert(id, None);
                Ok(())
            }
        }
    }

    fn get_child_knowledge(&self, id: Uuid) -> Result<Option<Uuid>, Error> {
        match self.children.get(&id) {
            None => Err(Error::ChildDoesNotExist(id)),
            Some(knowledge) => Ok(*knowledge)
        }
    }
}

struct SyncHandler {
    children: HashMap<Uuid, Option<Uuid>>,
    db_layer: Arc<DBLayer>
}

enum Error {
    ChildExists(Uuid),
    ChildDoesNotExist(Uuid)
}