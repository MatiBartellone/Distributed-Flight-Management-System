
pub struct DeleteQuery {
    pub table: String,
    pub where_clause: Vec<String>,
}

impl DeleteQuery {
    pub fn new() -> Self {
        Self{
            table: String::new(),
            where_clause: Vec::new(),
        }
    }
}