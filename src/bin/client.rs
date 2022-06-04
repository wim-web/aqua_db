use std::io::{stdin, stdout, BufWriter, Write};

use reqwest::blocking::Client;

const HELLO: &str = r"

▄▀█ █▀█ █░█ ▄▀█   █▀▄ █▄▄
█▀█ ▀▀█ █▄█ █▀█   █▄▀ █▄█

";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    output(HELLO)?;
    loop {
        output("> ")?;
        let mut input = String::new();
        stdin().read_line(&mut input)?;
        let response = communicate(&input)?;
        output(&format!("{}\n", response))?;
    }
}

fn output(message: &str) -> std::io::Result<()> {
    let out = stdout();
    let mut out = BufWriter::new(out.lock());
    write!(out, "{}", message)?;
    out.flush()?;

    Ok(())
}

fn communicate(input: &str) -> reqwest::Result<String> {
    let client = Client::new();

    let res = client
        .post("http://127.0.0.1:8080")
        .body(input.to_string())
        .send()?
        .text()?;

    Ok(res)
}
