use serde::{Deserialize, Serialize};

struct PagedVec<T> {
    current_page: Vec<T>,
    path: String
}