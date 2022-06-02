use std::io::{stdin, stdout, BufWriter, Write};

const hello: &str = r"

▄▀█ █▀█ █░█ ▄▀█   █▀▄ █▄▄
█▀█ ▀▀█ █▄█ █▀█   █▄▀ █▄█

";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    output(hello)?;
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
    let res = reqwest::blocking::get("http://127.0.0.1:8080")?.text()?;

    Ok(res)
}
