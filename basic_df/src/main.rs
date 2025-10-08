/* Example usage:
   cargo run -- path/to/file.parquet
*/
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let path = match args.next() {
        Some(p) => p,
        None => {
            eprintln!("Usage: basic_df <path_to.parquet>");
            return Ok(());
        }
    };

    let df = basic_df::read_parquet(&path)?;  // note: &path and ?

    println!("shape   : {:?}", df.shape());
    println!("names   : {:?}", df.column_names());
    println!("types   : {:?}", df.column_types());

    Ok(())
}

