//! Generate a small sample BSON file for smoke-testing bson2csv.
//! Usage: cargo run --release --example gen_sample -- <path> <count>
use std::fs::File;
use std::io::{BufWriter, Write};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let path = args.next().unwrap_or_else(|| "sample.bson".to_string());
    let count: usize = args
        .next()
        .unwrap_or_else(|| "1000".to_string())
        .parse()?;

    let f = File::create(&path)?;
    let mut w = BufWriter::new(f);
    for i in 0..count {
        let doc = bson::doc! {
            "_id": i as i64,
            "status": if i % 3 == 0 { "active" } else { "inactive" },
            "user": { "name": format!("user_{}", i), "age": 20 + (i % 50) as i32 },
            "tags": ["a", "b", "c"],
        };
        let mut buf = Vec::new();
        doc.to_writer(&mut buf)?;
        w.write_all(&buf)?;
    }
    w.flush()?;
    println!("Wrote {count} documents to {path}");
    Ok(())
}
