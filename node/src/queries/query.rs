use std::any::Any;
use serde::{Deserialize, Serialize};
use crate::queries::delete_query::DeleteQuery;
use crate::queries::insert_query::InsertQuery;
use crate::queries::select_query::SelectQuery;
use crate::queries::update_query::UpdateQuery;
use crate::queries::use_query::UseQuery;
use crate::utils::errors::Errors;

pub trait Query : Any{
    fn run(&self) -> Result<String, Errors>;
    fn as_any(&self) -> &dyn Any;
}

#[derive(Serialize, Deserialize)]
pub enum QueryEnum{
    Insert(InsertQuery),
    Delete(DeleteQuery),
    Update(UpdateQuery),
    Select(SelectQuery),
    Use(UseQuery),
}

impl QueryEnum{
    pub fn into_query(self) -> Box<dyn Query> {
        match self {
            QueryEnum::Insert(query) => Box::new(query),
            QueryEnum::Delete(query) => Box::new(query),
            QueryEnum::Update(query) => Box::new(query),
            QueryEnum::Select(query) => Box::new(query),
            QueryEnum::Use(query) => Box::new(query),
        }
    }

    pub fn from_query(query: &Box<dyn Query>) -> Option<Self> {
        if let Some(insert_query) = query.as_any().downcast_ref::<InsertQuery>(){
            Some(QueryEnum::Insert(insert_query.clone()))
        } else if let Some(delete_query) = query.as_any().downcast_ref::<DeleteQuery>(){
            Some(QueryEnum::Delete(delete_query.clone()))
        } else if let Some(update_query) = query.as_any().downcast_ref::<UpdateQuery>(){
            Some(QueryEnum::Update(update_query.clone()))
        } else if let Some(select_query) = query.as_any().downcast_ref::<SelectQuery>(){
            Some(QueryEnum::Select(select_query.clone()))
        } else if let Some(use_query) = query.as_any().downcast_ref::<UseQuery>(){
            Some(QueryEnum::Use(use_query.clone()))
        } else {
            None
        }
    }
}


