use std::collections::HashMap;

pub trait DataFrame: Send + Sync {
    fn shape(&self) -> (usize, usize);
    fn column_names(&self) -> Vec<String>;
    fn column_types(&self) -> HashMap<String, String>;

    /// Read a Float64 column into Vec<f64>, replacing NULL with NaN
    fn read_column_f64(&self, column_name: &str) -> Vec<f64>;

    /// Read the first column (index) of type Timestamp(Microsecond, Some("UTC")) into Vec<i64>
    fn read_index_microsecond(&self) -> Vec<i64>;
}
