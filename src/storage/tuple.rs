use std::collections::HashMap;

use crate::catalog::*;

pub const TUPLE_HEADER_SIZE: usize = 8;

#[derive(Default, Debug)]
pub struct Tuple {
    pub header: TupleHeader,
    pub body: TupleBody,
}

impl Tuple {
    pub fn new() -> Self {
        Self {
            header: TupleHeader { deleted: 0 },
            body: Default::default(),
        }
    }

    pub fn fill(&mut self, raw: &[u8], columns: &Vec<Column>) {
        self.header.fill(&raw[..TUPLE_HEADER_SIZE]);
        self.body.fill(&raw[TUPLE_HEADER_SIZE..], columns);
    }

    pub fn add_attribute(&mut self, name: &str, types: AttributeType) {
        self.body.attributes.insert(name.to_string(), types);
    }

    pub fn raw(&self, columns: &Vec<Column>) -> Vec<u8> {
        let mut b = vec![];
        b.append(&mut self.header.raw());
        b.append(&mut self.body.raw(columns));

        b
    }
}

#[derive(Default, Debug)]
// 8byte
// deleted - 1byte
pub struct TupleHeader {
    pub deleted: u8,
}

impl TupleHeader {
    fn fill(&mut self, raw: &[u8]) {
        let mut deleted_byte = [0_u8; 1];
        deleted_byte.clone_from_slice(&raw[..1]);
        self.deleted = u8::from_be_bytes(deleted_byte);
    }

    fn raw(&self) -> Vec<u8> {
        let deleted_byte = self.deleted.to_be_bytes().to_vec();
        let padding = vec![0_u8; 8 - deleted_byte.len()];

        [deleted_byte, padding].concat()
    }
}

#[derive(Default, Debug)]
pub struct TupleBody {
    pub attributes: HashMap<String, AttributeType>,
}

impl TupleBody {
    fn fill(&mut self, raw: &[u8], columns: &Vec<Column>) {
        let mut offset = 0;
        for c in columns {
            let t = match c.types.as_str() {
                "int" => {
                    let mut bytes = [0_u8; 4];
                    bytes.clone_from_slice(&raw[offset..(offset + 4)]);
                    let num = i32::from_be_bytes(bytes);
                    offset += 4;
                    AttributeType::Int(num)
                }
                "text" => {
                    let mut length_bytes = [0_u8; 1];
                    length_bytes.clone_from_slice(&raw[offset..(offset + 1)]);
                    let length = u8::from_be_bytes(length_bytes);
                    let mut str_bytes = [0_u8; 255];
                    str_bytes.copy_from_slice(&raw[(offset + 1)..(offset + 256)]);
                    let str_bytes = &str_bytes[..(length as usize)];
                    let str = String::from_utf8(str_bytes.to_vec()).unwrap();
                    offset += 256;
                    AttributeType::Text(str)
                }
                s => panic!("{} is not defined", s),
            };
            self.attributes.insert(c.name.clone(), t);
        }
    }

    fn raw(&self, columns: &Vec<Column>) -> Vec<u8> {
        let mut bytes = vec![];

        for c in columns {
            let types = self
                .attributes
                .get(&c.name)
                .and_then(|t| match c.types.as_str() {
                    "int" => match &t {
                        AttributeType::Int(v) => Some(t),
                        _ => None,
                    },
                    "text" => match &t {
                        AttributeType::Text(v) => Some(t),
                        _ => None,
                    },
                    _ => None,
                })
                .unwrap();

            match types {
                AttributeType::Int(v) => {
                    let mut b = v.to_be_bytes().to_vec();
                    bytes.append(&mut b);
                }
                AttributeType::Text(v) => {
                    let len = v.len();
                    let mut len_byte = [len as u8].to_vec();
                    bytes.append(&mut len_byte);
                    let mut str_bytes = v.as_bytes().to_vec();
                    bytes.append(&mut str_bytes);
                    let mut padding = vec![0_u8; 255 - len];
                    bytes.append(&mut padding);
                }
            }
        }

        bytes
    }
}
