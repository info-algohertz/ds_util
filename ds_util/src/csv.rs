use crate::dataframe::DataFrame;
use std::collections::HashMap;

const SEPARATOR: chat = ',';

//TODO
pub fn read_csv(path: &str, column_names: Option<Vec<String>>) -> Result<Box<dyn DataFrame>, Box<dyn std::error::Error>> {
    Ok(Box::new(CsvDataFrame {
        path: path.to_string(),
    }))
}

//TODO
struct CsvDataFrame {
    path: String,
    column_data: HashMap<String, Vec<String>>,
}

impl DataFrame for CsvDataFrame {
    //TODO
    fn shape(&self) -> (usize, usize) {
        panic!("Not implemented!");
    }

    //TODO
    fn column_names(&self) -> Vec<String> {
        panic!("Not implemented!");
    }

    fn column_types(&self) -> HashMap<String, String> {
        panic!("Not implemented!");
    }

    //TODO
    fn read_column_i64(&self, column_name: &str) -> Vec<f64> {
        panic!("Not implemented!");
    }

    //TODO
    fn read_column_f64(&self, column_name: &str) -> Vec<f64> {
        panic!("Not implemented!");
    }        

    fn read_index_microsecond(&self) -> Vec<i64> {
        panic!("Not implemented!");
    }
}
