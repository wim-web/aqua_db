use std::{
    fs::File,
    io::{BufRead, BufReader, BufWriter, Read, Write},
    net::{TcpListener, TcpStream},
    vec,
};

use aqua_db::{
    catalog::Catalog,
    executor::Executor,
    query::{ExecuteType, InsertInput, Parser, SelectInput},
    storage::{buffer_pool_manager::BufferPoolManager, replacer::LruReplacer},
};

fn main() -> Result<(), anyhow::Error> {
    let mut json_file = File::open("schema.json").unwrap();
    let mut buf = Vec::new();
    json_file.read_to_end(&mut buf).unwrap();
    let json = String::from_utf8(buf).unwrap();
    let catalog = Catalog::from_json(&json);

    let parser = Parser::new(&catalog);
    let manager = BufferPoolManager::new(10, "./data".to_string(), catalog.clone());
    let mut executor = Executor::new(manager);

    let listener = TcpListener::bind("127.0.0.1:8080")?;

    for stream in listener.incoming() {
        let read = stream?;
        let write = read.try_clone()?;

        let mut writer = BufWriter::new(&write);

        let response_text = match read_handler(&read, &mut executor, &parser) {
            Ok(s) => s,
            Err(e) => format!("{}", e),
        };

        let response = format!("HTTP/1.1 200 OK\r\n\r\n{}", response_text);
        writer.write_all(response.as_bytes())?;

        if response_text == "exit" {
            exit_handler(&mut executor)?;
            break;
        }
    }

    Ok(())
}

fn read_handler(
    stream: &TcpStream,
    executor: &mut Executor<LruReplacer>,
    parser: &Parser,
) -> Result<String, anyhow::Error> {
    let mut reader = BufReader::new(stream);

    let mut length = 0;

    for x in reader.by_ref().lines() {
        let x = x?;
        if x.is_empty() {
            break;
        }

        if x.starts_with("POST") {
            continue;
        }

        let header = x.split(':').collect::<Vec<&str>>();

        if header[0] == "content-length" {
            length = header[1].trim().parse::<u32>()?;
        }
    }

    let mut buf = vec![0_u8; (length - 1) as usize];
    let _ = reader.read(&mut buf[..])?;

    let query = std::str::from_utf8(&buf)?;

    let response_text = match parser.parse(query)? {
        ExecuteType::Select(SelectInput { table_name }) => {
            let mut records = Vec::new();
            executor.scan(&table_name, &mut records)?;
            let mut s = String::new();
            let len = records.len();
            for r in records {
                s.push_str(format!("{:?}\n", r).as_str());
            }
            s.push_str(format!("total: {}", len).as_str());
            s
        }
        ExecuteType::Insert(InsertInput {
            attributes,
            table_name,
        }) => {
            executor.insert(&attributes, &table_name)?;
            "success".to_string()
        }
        ExecuteType::Exit => "exit".to_string(),
    };

    Ok(response_text)
}

fn exit_handler(executor: &mut Executor<LruReplacer>) -> Result<(), anyhow::Error> {
    executor.all_flush()?;
    Ok(())
}
