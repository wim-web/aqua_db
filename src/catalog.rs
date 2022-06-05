use crate::storage::tuple::*;
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Catalog {
    #[serde(rename = "schemas")]
    pub schemas: Vec<Schema>,
    #[serde(skip)]
    pub map: HashMap<String, usize>,
}

impl Catalog {
    pub fn from_json(json: &str) -> Self {
        let mut c: Catalog = serde_json::from_str(json).unwrap();

        c.schemas.iter().enumerate().for_each(|(index, schema)| {
            c.map.insert(schema.table.name.clone(), index);
        });

        c
    }

    pub fn get_schema_by_table_name(&self, table_name: &str) -> Option<&Schema> {
        let index = *self.map.get(table_name)?;
        Some(&self.schemas[index])
    }

    pub fn exist_table(&self, table_name: &str) -> bool {
        self.map.get(table_name).is_some()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Schema {
    pub table: Table,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

impl Table {
    pub fn tuple_size(&self) -> usize {
        TUPLE_HEADER_SIZE
            + self
                .columns
                .iter()
                .fold(0, |acc, c| match c.types.as_str() {
                    "int" => acc + 4,
                    "text" => acc + 256,
                    _ => acc,
                })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Column {
    pub types: String,
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum AttributeType {
    Int(i32),
    Text(String),
}

#[cfg(test)]
mod tests {

    use super::*;

    const JSON: &str = r#"{
        "schemas": [
            {
                "table": {
                    "name": "table1",
                    "columns": [
                        {
                            "types": "int",
                            "name": "column_int"
                        },
                        {
                            "types": "text",
                            "name": "column_text"
                        }
                    ]
                }
            }
        ]
    }"#;

    #[test]
    fn catalog_from_json() {
        let c = Catalog::from_json(JSON);

        // assert table num
        assert_eq!(1, c.schemas.len());

        let schema = c.get_schema_by_table_name("table1").unwrap();

        assert_eq!("table1", schema.table.name);

        for column in &schema.table.columns {
            match column.types.as_str() {
                "int" => assert_eq!(column.name, "column_int"),
                "text" => assert_eq!(column.name, "column_text"),
                s => panic!("{} is undefined types", s),
            };
        }
    }

    #[test]
    fn catalog_tuple_size() {
        let c = Catalog::from_json(JSON);
        let schema = c.get_schema_by_table_name("table1").unwrap();
        let tuple_size = schema.table.tuple_size();

        assert_eq!(tuple_size, 268)
    }
}
