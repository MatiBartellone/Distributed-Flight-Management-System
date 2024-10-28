use crate::queries::alter_table_query::AlterTableQuery;
use crate::queries::create_keyspace_query::CreateKeyspaceQuery;
use crate::queries::create_table_query::CreateTableQuery;
use crate::queries::delete_query::DeleteQuery;
use crate::queries::drop_keyspace_query::DropKeySpaceQuery;
use crate::queries::drop_table_query::DropTableQuery;
use crate::queries::insert_query::InsertQuery;
use crate::queries::select_query::SelectQuery;
use crate::queries::update_query::UpdateQuery;
use crate::queries::use_query::UseQuery;
use crate::utils::errors::Errors;
use serde::{Deserialize, Serialize};
use std::any::Any;

pub trait Query: Any {
    fn run(&self) -> Result<Vec<u8>, Errors>;
    fn get_partition(&self) -> Result<Option<Vec<String>>, Errors>;
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
    DropKeyspace(DropKeySpaceQuery),
    DropTable(DropTableQuery),
    AlterTable(AlterTableQuery),
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
            QueryEnum::DropKeyspace(query) => Box::new(query),
            QueryEnum::DropTable(query) => Box::new(query),
            QueryEnum::AlterTable(query) => Box::new(query),
        }
    }

    #[allow(clippy::borrowed_box)]
    pub fn from_query(query: &Box<dyn Query>) -> Option<Self> {
        if let Some(insert_query) = query.as_any().downcast_ref::<InsertQuery>() {
            return Some(QueryEnum::Insert(insert_query.to_owned()));
        } else if let Some(delete_query) = query.as_any().downcast_ref::<DeleteQuery>() {
            return Some(QueryEnum::Delete(delete_query.to_owned()));
        } else if let Some(update_query) = query.as_any().downcast_ref::<UpdateQuery>() {
            return Some(QueryEnum::Update(update_query.to_owned()));
        } else if let Some(select_query) = query.as_any().downcast_ref::<SelectQuery>() {
            return Some(QueryEnum::Select(select_query.to_owned()));
        } else if let Some(use_query) = query.as_any().downcast_ref::<UseQuery>() {
            return Some(QueryEnum::Use(use_query.to_owned()));
        } else if let Some(create_keyspace) = query.as_any().downcast_ref::<CreateKeyspaceQuery>() {
            return Some(QueryEnum::CreateKeyspace(create_keyspace.to_owned()));
        } else if let Some(create_table) = query.as_any().downcast_ref::<CreateTableQuery>() {
            return Some(QueryEnum::CreateTable(create_table.to_owned()));
        } else if let Some(drop_keyspace) = query.as_any().downcast_ref::<DropKeySpaceQuery>() {
            return Some(QueryEnum::DropKeyspace(drop_keyspace.to_owned()));
        } else if let Some(drop_table) = query.as_any().downcast_ref::<DropTableQuery>() {
            return Some(QueryEnum::DropTable(drop_table.to_owned()));
        } else if let Some(alter_table) = query.as_any().downcast_ref::<AlterTableQuery>() {
            return Some(QueryEnum::AlterTable(alter_table.to_owned()));
        }
        None
    }
}
