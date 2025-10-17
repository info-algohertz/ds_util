/* Examples using ds_util functions

   Example usage:
   cargo run -- $HOME/data.parquet
   cargo run -- $HOME/data.csv
*/

use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let path = match args.next() {
        Some(p) => p,
        None => {
            eprintln!("Usage: ds_util <path_to.parquet>");
            return Ok(());
        }
    };

    let df = ds_util::read_parquet(&path)?;

    println!("shape   : {:?}", df.shape());
    println!("names   : {:?}", df.column_names());
    println!("types   : {:?}", df.column_types());

    let v = df.read_column_f64(&df.column_names()[0]);
    dbg!(&v[0..10]);

    let index = df.read_index_microsecond();
    dbg!(&index[0..10]);

    Ok(())
}
