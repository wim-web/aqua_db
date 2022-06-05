use std::collections::HashMap;

use crate::catalog::{AttributeType, Catalog, Column};

pub struct Parser<'a> {
    catalog: &'a Catalog,
}

#[derive(PartialEq, Debug)]
pub enum ExecuteType {
    Select(SelectInput),
    Insert(InsertInput),
}

#[derive(PartialEq, Debug)]
pub struct SelectInput {
    pub table_name: String,
}

#[derive(PartialEq, Debug)]
pub struct InsertInput {
    pub table_name: String,
    pub attributes: HashMap<String, AttributeType>,
}

impl<'a> Parser<'a> {
    pub fn new(catalog: &'a Catalog) -> Self {
        Self { catalog }
    }

    pub fn parse(&self, query: &str) -> Result<ExecuteType, anyhow::Error> {
        if !query.ends_with(';') {
            return Err(anyhow::anyhow!("expect end with ;"));
        }

        // remove ;
        let mut query = query.to_string();
        query.pop();

        let splitted: Vec<&str> = query.split(' ').collect();

        match splitted[0] {
            "select" => self.parse_select(&splitted),
            "insert" => self.parse_insert(&splitted),
            t => Err(anyhow::anyhow!("not expected {}", t)),
        }
    }

    fn parse_select(&self, tokens: &[&str]) -> Result<ExecuteType, anyhow::Error> {
        if tokens.len() < 4 {
            return Err(anyhow::anyhow!("select query something wrong"));
        }

        let table_name = tokens[3].to_string();

        if !self.catalog.exist_table(&table_name) {
            return Err(anyhow::anyhow!("{} not exist", table_name));
        }

        Ok(ExecuteType::Select(SelectInput { table_name }))
    }

    fn parse_insert(&self, tokens: &[&str]) -> Result<ExecuteType, anyhow::Error> {
        if tokens.len() < 6 {
            return Err(anyhow::anyhow!("insert query something wrong"));
        }

        let table_name = tokens[2].to_string();

        let table = &self
            .catalog
            .get_schema_by_table_name(&table_name)
            .ok_or_else(|| anyhow::anyhow!("{} not exist", table_name))?
            .table;

        let mut raw_attributes = HashMap::new();
        let mut attributes = HashMap::new();

        // gather attribute
        'o: for (i, &token) in tokens.iter().enumerate() {
            if token != "(" {
                continue;
            }

            for &x in &tokens[i + 1..] {
                if x == ")" {
                    break 'o;
                }

                // insert into users ( id=1 name='hoge' );

                let v: Vec<&str> = x.split('=').collect();

                if v.len() != 2 {
                    return Err(anyhow::anyhow!(
                        "Specify an attribute like column_name=value"
                    ));
                }

                let c_name = v[0];
                let value = v[1];

                raw_attributes.insert(c_name, value);
            }

            return Err(anyhow::anyhow!("not found )"));
        }

        for Column { name, types } in &table.columns {
            let &value = raw_attributes
                .get(name.as_str())
                .ok_or_else(|| anyhow::anyhow!("{} is not found", name))?;

            let t = match types.as_str() {
                "int" => Ok(AttributeType::Int(value.parse().unwrap())),
                "text" => {
                    let mut s = value.to_string();
                    // remove '
                    s.remove(0);
                    s.pop();
                    Ok(AttributeType::Text(s))
                }
                _ => Err(anyhow::anyhow!("not found )")),
            }?;

            attributes.insert(name.clone(), t);
        }

        Ok(ExecuteType::Insert(InsertInput {
            table_name,
            attributes,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const JSON: &str = r#"{
        "schemas": [
            {
                "table": {
                    "name": "query_test",
                    "columns": [
                        {
                            "types": "int",
                            "name": "number"
                        },
                        {
                            "types": "text",
                            "name": "text"
                        }
                    ]
                }
            }
        ]
    }"#;

    #[test]
    fn query_parse_select() {
        let catalog = Catalog::from_json(JSON);
        let p = Parser::new(&catalog);
        let query = "select * from query_test;";

        let e_type = p.parse(query).unwrap();

        assert_eq!(
            e_type,
            ExecuteType::Select(SelectInput {
                table_name: "query_test".to_string()
            })
        );
    }

    #[test]
    fn query_parse_insert() {
        let catalog = Catalog::from_json(JSON);
        let p = Parser::new(&catalog);
        let query = "insert into query_test ( number=1 text='hoge' );";

        let e_type = p.parse(query).unwrap();

        let mut attributes = HashMap::new();
        attributes.insert("number".to_string(), AttributeType::Int(1));
        attributes.insert("text".to_string(), AttributeType::Text("hoge".to_string()));

        assert_eq!(
            e_type,
            ExecuteType::Insert(InsertInput {
                table_name: "query_test".to_string(),
                attributes
            })
        );
    }

    #[test]
    fn query_parse_end_with_semicolon() {
        let catalog = Catalog::from_json(JSON);
        let p = Parser::new(&catalog);
        let query = "select id, name from users";

        assert!(p.parse(query).is_err());
    }

    #[test]
    fn query_parse_not_support_type() {
        let catalog = Catalog::from_json(JSON);
        let p = Parser::new(&catalog);
        let query = "update users";

        assert!(p.parse(query).is_err());
    }
}
