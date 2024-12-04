pub struct QueryBuilder {
    query_type: String,
    table: String,
    columns: Vec<String>,
    values: Vec<String>,
    conditions: Vec<String>,
    logical_operators: Vec<String>,
    order_by: Option<(String, String)>,
    if_condition: Option<String>,
}

impl QueryBuilder {
    pub fn new(query_type: &str, table: &str) -> Self {
        Self {
            query_type: query_type.to_string(),
            table: table.to_string(),
            columns: Vec::new(),
            values: Vec::new(),
            conditions: Vec::new(),
            logical_operators: Vec::new(),
            order_by: None,
            if_condition: None,
        }
    }

    /// Method to set the columns to be selected
    pub fn select(mut self, columns: Vec<&str>) -> Self {
        self.columns = columns.into_iter().map(|col| col.to_string()).collect();
        self
    }

    /// Method to set the columns and values to be inserted
    pub fn insert(mut self, columns: Vec<&str>, values: Vec<&str>) -> Self {
        self.columns = columns.into_iter().map(|col| col.to_string()).collect();
        self.values = values.into_iter().map(|val| val.to_string()).collect();
        self
    }

    /// Method to set the columns and values to be updated    
    pub fn update(mut self, set: Vec<(&str, &str)>) -> Self {
        self.columns.clear();
        for (col, val) in set {
            self.columns.push(format!("{} = {}", col, val));
        }
        self
    }

    /// Method to push a condition to the query
    pub fn where_condition(mut self, condition: &str, operator: Option<&str>) -> Self {
        self.conditions.push(condition.to_string());
        self.logical_operators.push(operator.unwrap_or("AND").to_string());
        self
    }

    /// Method to set the order by column and direction
    pub fn order_by(mut self, column: &str, direction: Option<&str>) -> Self {
        let direction = direction.unwrap_or("ASC").to_string();
        self.order_by = Some((column.to_string(), direction));
        self
    }

    /// Method to set the IF condition
    pub fn if_condition(mut self, condition: &str) -> Self {
        self.if_condition = Some(condition.to_string());
        self
    }

    /// Method to set the query type to SELECT
    pub fn delete(mut self) -> Self {
        self.query_type = "DELETE".to_string();
        self
    }

    /// Method to build the query 
    pub fn build(self) -> String {
        let mut query = match self.query_type.as_str() {
            "SELECT" => format!("SELECT {} FROM {}", self.columns.join(", "), self.table),
            "INSERT" => format!("INSERT INTO {} ({}) VALUES ({})", self.table, self.columns.join(", "), self.values.join(", ")),
            "UPDATE" => format!("UPDATE {} SET {}", self.table, self.columns.join(", ")),
            "DELETE" => format!("DELETE FROM {}", self.table),
            _ => return "Tipo de consulta desconocido".to_string(),
        };

        if !self.conditions.is_empty() {
            query.push_str(" WHERE ");
            let mut condition_parts = Vec::new();
            for (i, condition) in self.conditions.iter().enumerate() {
                if i > 0 {
                    condition_parts.push(self.logical_operators[i - 1].to_string());
                }
                condition_parts.push(condition.to_string());
            }
            query.push_str(&condition_parts.join(" "));
        }

        if let Some((column, direction)) = self.order_by {
            query.push_str(&format!(" ORDER BY {} {}", column, direction));
        }

        if let Some(if_condition) = self.if_condition {
            query.push_str(&format!(" IF {}", if_condition));
        }

        query
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_query() {
        let query = QueryBuilder::new("SELECT", "flight")
            .select(vec!["id", "status", "location"])
            .where_condition("status = 'delayed'", None)
            .order_by("id", Some("DESC"))
            .build();

        let expected = "SELECT id, status, location FROM flight WHERE status = 'delayed' ORDER BY id DESC";
        assert_eq!(query, expected);
    }

    #[test]
    fn test_insert_query() {
        let query = QueryBuilder::new("INSERT", "flight")
            .insert(vec!["id", "status", "location"], vec!["1", "'on time'", "'New York'"])
            .build();

        let expected = "INSERT INTO flight (id, status, location) VALUES (1, 'on time', 'New York')";
        assert_eq!(query, expected);
    }

    #[test]
    fn test_update_query() {
        let query = QueryBuilder::new("UPDATE", "flight")
            .update(vec![("status", "'delayed'"), ("location", "'Chicago'")])
            .where_condition("id = 1", None)
            .if_condition("status = 'on time'")
            .build();

        let expected = "UPDATE flight SET status = 'delayed', location = 'Chicago' WHERE id = 1 IF status = 'on time'";
        assert_eq!(query, expected);
    }

    #[test]
    fn test_delete_query() {
        let query = QueryBuilder::new("DELETE", "flight")
            .where_condition("status = 'delayed'", None)
            .build();

        let expected = "DELETE FROM flight WHERE status = 'delayed'";
        assert_eq!(query, expected);
    }

    #[test]
    fn test_order_by_default_asc() {
        let query = QueryBuilder::new("SELECT", "flight")
            .select(vec!["id", "status", "location"])
            .order_by("id", None)
            .build();

        let expected = "SELECT id, status, location FROM flight ORDER BY id ASC";
        assert_eq!(query, expected);
    }

    #[test]
    fn test_delete_query_with_multiple_conditions() {
        let query = QueryBuilder::new("DELETE", "flight")
            .where_condition("status = 'delayed'", Some("AND"))
            .where_condition("location = 'New York'", None)
            .build();

        let expected = "DELETE FROM flight WHERE status = 'delayed' AND location = 'New York'";
        assert_eq!(query, expected);
    }

    #[test]
    fn test_if_condition_at_end() {
        let query = QueryBuilder::new("UPDATE", "flight")
            .update(vec![("status", "'delayed'")])
            .where_condition("id = 1", None)
            .if_condition("EXISTS")
            .build();

        let expected = "UPDATE flight SET status = 'delayed' WHERE id = 1 IF EXISTS";
        assert_eq!(query, expected);
    }
}
