/* Examples using ds_util functions

Example usage:
cargo run -- $HOME/data.parquet $HOME/data.csv $HOME/out.parquet
*/

use std::collections::HashMap;
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let input_parquet_path = &args[1];
    let output_parquet_path = &args[3];

    let df = ds_util::read_parquet(&input_parquet_path)?;

    println!("shape   : {:?}", df.shape());
    println!("names   : {:?}", df.column_names());
    println!("types   : {:?}", df.column_types());

    let v = df.read_column_f64(&df.column_names()[0]);
    dbg!(&v[0..10]);

    let index = df.read_index_microsecond();
    dbg!(&index[0..10]);

    // Write out to a parquet file.
    let timestamps = vec![1609459200000, 1609545600000, 1609632000000];

    let mut data = HashMap::new();
    data.insert("temperature".to_string(), vec![20.5, 21.3, 19.8]);
    data.insert("humidity".to_string(), vec![65.0, 68.5, 70.2]);

    ds_util::write_parquet(output_parquet_path, Some(timestamps), data)?;

    println!("Data written to the output parquet file successfully!");

    Ok(())
}
