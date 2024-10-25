use crate::queries::delete_query::DeleteQuery;
use crate::queries::insert_query::InsertQuery;
use crate::queries::select_query::SelectQuery;
use crate::queries::update_query::UpdateQuery;
use crate::queries::use_query::UseQuery;
use crate::utils::errors::Errors;
use serde::{Deserialize, Serialize};
use std::any::Any;
use crate::queries::create_keyspace_query::CreateKeyspaceQuery;
use crate::queries::create_table_query::CreateTableQuery;

pub trait Query: Any {
    fn run(&self) -> Result<Vec<u8>, Errors>;
    fn get_primary_key(&self) -> Result<Option<Vec<String>>, Errors>;
    fn get_keyspace(&self) -> Result<String, Errors>;
    fn set_table(&mut self) -> Result<(), Errors>;
    fn as_any(&self) -> &dyn Any;
}

#[derive(Serialize, Deserialize)]
pub enum QueryEnum {
    Insert(InsertQuery),
    Delete(DeleteQuery),
    Update(UpdateQuery),
    Select(SelectQuery),
    Use(UseQuery),
    CreateKeyspace(CreateKeyspaceQuery),
    CreateTable(CreateTableQuery),
}

impl QueryEnum {
    pub fn into_query(self) -> Box<dyn Query> {
        match self {
            QueryEnum::Insert(query) => Box::new(query),
            QueryEnum::Delete(query) => Box::new(query),
            QueryEnum::Update(query) => Box::new(query),
            QueryEnum::Select(query) => Box::new(query),
            QueryEnum::Use(query) => Box::new(query),
            QueryEnum::CreateKeyspace(query) => Box::new(query),
            QueryEnum::CreateTable(query) => Box::new(query),
        }
    }

    #[allow(clippy::borrowed_box)]
    pub fn from_query(query: &Box<dyn Query>) -> Option<Self> {
        if let Some(insert_query) = query.as_any().downcast_ref::<InsertQuery>() {
            return Some(QueryEnum::Insert(insert_query.clone()));
        } else if let Some(delete_query) = query.as_any().downcast_ref::<DeleteQuery>() {
            return Some(QueryEnum::Delete(delete_query.clone()));
        } else if let Some(update_query) = query.as_any().downcast_ref::<UpdateQuery>() {
            return Some(QueryEnum::Update(update_query.clone()));
        } else if let Some(select_query) = query.as_any().downcast_ref::<SelectQuery>() {
            return Some(QueryEnum::Select(select_query.clone()));
        } else if let Some(use_query) = query.as_any().downcast_ref::<UseQuery>() {
            return Some(QueryEnum::Use(use_query.clone()));
        }else if let Some(create_keyspace) = query.as_any().downcast_ref::<CreateKeyspaceQuery>() {
            return Some(QueryEnum::CreateKeyspace(create_keyspace.clone()));
        } else if let Some(create_table) = query.as_any().downcast_ref::<CreateTableQuery>() {
            return Some(QueryEnum::CreateTable(create_table.clone()));
        }
        None
    }
}
