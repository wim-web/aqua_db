use super::tuple::*;
use crate::catalog::*;

pub const PAGE_SIZE: usize = 4096;
const PAGE_HEADER_SIZE: usize = 32;

#[derive(Debug)]
pub struct Page {
    pub id: PageID,
    pub header: PageHeader,
    pub body: Vec<Tuple>,
    pub size: usize,
}

impl Page {
    pub fn fill(&mut self, raw: &[u8], schema: &Schema) {
        assert!(raw.len() == PAGE_SIZE);
        self.header.fill(&raw[..PAGE_HEADER_SIZE]);

        let mut v: Vec<Tuple> = Vec::with_capacity(self.header.tuple_count as usize);

        let mut offset = PAGE_HEADER_SIZE;
        let table = &schema.table;
        let tuple_size = table.tuple_size();

        for _ in (0..self.header.tuple_count) {
            let mut tuple = Tuple::default();
            tuple.fill(&raw[offset..(offset + tuple_size)], &table.columns);
            v.push(tuple);
            offset += tuple_size;
        }

        self.body = v;
    }

    pub fn add_tuple(&mut self, tuple: Tuple) {
        self.header.tuple_count += 1;
        self.body.push(tuple);
    }

    pub fn raw(&self, schema: &Schema) -> Vec<u8> {
        let mut b = vec![];
        b.append(&mut self.header.raw());

        for t in &self.body {
            b.append(&mut t.raw(&schema.table.columns));
        }

        if PAGE_SIZE > b.len() {
            b.append(&mut vec![0_u8; PAGE_SIZE - b.len()]);
        }

        b
    }
}

impl Default for Page {
    fn default() -> Self {
        Self {
            id: PageID(0),
            size: PAGE_SIZE,
            header: PageHeader { tuple_count: 0 },
            body: Vec::new(),
        }
    }
}

#[derive(Hash, PartialEq, Eq, Clone, Debug, Copy)]
pub struct PageID(pub usize);

impl PageID {
    pub fn value(&self) -> usize {
        self.0
    }

    pub fn offset(&self) -> usize {
        PAGE_SIZE * self.0
    }
}

#[derive(Default, Debug)]
// 32byte
// tuple_count - 4byte
// The remaining bytes are reserved space
pub struct PageHeader {
    pub tuple_count: u32,
}

impl PageHeader {
    fn fill(&mut self, raw: &[u8]) {
        let mut tuple_count_byte = [0_u8; 4];
        tuple_count_byte.clone_from_slice(&raw[..4]);
        self.tuple_count = u32::from_be_bytes(tuple_count_byte);
    }

    fn raw(&self) -> Vec<u8> {
        let mut b = vec![];
        b.append(&mut self.tuple_count.to_be_bytes().to_vec());
        b.append(&mut vec![0_u8; 32 - 4]);
        b
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
                        }
                    ]
                }
            }
        ]
    }"#;

    #[test]
    fn page_serde() {
        let c = Catalog::from_json(json);
        let schema = c.get_schema_by_table_name("table1").unwrap();

        let mut page = Page::default();
        let mut tuple = Tuple::new();
        tuple.add_attribute("column_int", AttributeType::Int(1));
        tuple.add_attribute("column_text", AttributeType::Text("text".to_string()));
        page.add_tuple(tuple);

        let page_raw = page.raw(schema);

        assert_eq!(PAGE_SIZE, page_raw.len());

        let mut page = Page::default();
        page.fill(&page_raw, schema);

        assert_eq!(1, page.header.tuple_count);
        for b in page.body {
            assert_eq!(0, b.header.deleted);
            match b.body.attributes.get("column_int").unwrap() {
                AttributeType::Int(v) => assert_eq!(*v, 1),
                _ => panic!("expected int, but"),
            }
            match b.body.attributes.get("column_text").unwrap() {
                AttributeType::Text(v) => assert_eq!(*v, "text"),
                _ => panic!("expected text, but"),
            }
        }
    }
}
