use crate::dataframe::DataFrame;
use std::collections::HashMap;

pub fn read_csv(path: &str) -> Result<Box<dyn DataFrame>, Box<dyn std::error::Error>> {
    Ok(Box::new(CsvDataFrame {
        path: path.to_string(),
    }))
}

struct CsvDataFrame {
    path: String,
}

impl DataFrame for CsvDataFrame {
    fn shape(&self) -> (usize, usize) {
        panic!("Not implemented!");
        //0usize, 0usize
    }

    fn column_names(&self) -> Vec<String> {
        panic!("Not implemented!");
        //Vec::new()
    }

    fn column_types(&self) -> HashMap<String, String> {
        panic!("Not implemented!");
        //HashMap::new()
    }

    fn read_column_f64(&self, column_name: &str) -> Vec<f64> {
        panic!("Not implemented!");
        //Vec::new()
    }

    fn read_index_microsecond(&self) -> Vec<i64> {
        panic!("Not implemented!");
        //Vec::new()
    }
}
