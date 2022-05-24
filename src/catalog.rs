use std::collections::HashMap;

use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct Catalog {
    #[serde(rename = "schemas")]
    schemas: Vec<Schema>,
    #[serde(skip)]
    map: HashMap<String, usize>,
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
}

#[derive(Serialize, Deserialize, Debug)]
struct Schema {
    table: Table,
}

#[derive(Serialize, Deserialize, Debug)]
struct Table {
    name: String,
    columns: Vec<Column>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Column {
    types: ColumnType,
    name: String,
}

#[derive(Serialize, Deserialize, Debug)]
enum ColumnType {
    #[serde(rename = "int")]
    Int,
    #[serde(rename = "text")]
    Text,
    #[serde(rename = "bool")]
    Bool,
}

#[cfg(test)]
mod tests {
    use crate::catalog::ColumnType;

    use super::Catalog;

    const json: &str = r#"{
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
                        },
                        {
                            "types": "bool",
                            "name": "column_bool"
                        }
                    ]
                }
            }
        ]
    }"#;

    #[test]
    fn catalog_deserialize() {
        let c = Catalog::from_json(json);

        assert_eq!(1, c.schemas.len());

        let schema = c.get_schema_by_table_name("table1").unwrap();

        assert_eq!("table1", schema.table.name);

        for column in &schema.table.columns {
            let c_name = match column.types {
                ColumnType::Int => "column_int",
                ColumnType::Text => "column_text",
                ColumnType::Bool => "column_bool",
            };

            assert_eq!(column.name, c_name);
        }
    }
}
