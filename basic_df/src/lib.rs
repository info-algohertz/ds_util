use std::collections::HashMap;
use std::fs::File;

use arrow::array::Float64Array;
use arrow::datatypes::SchemaRef;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};

pub trait DataFrame: Send + Sync {
    fn shape(&self) -> (usize, usize);
    fn column_names(&self) -> Vec<String>;
    fn column_types(&self) -> HashMap<String, String>;
    fn read_column_f64(&self, column_name: &str) -> Vec<f64>;
}

pub fn read_parquet(path: &str) -> Result<Box<dyn DataFrame>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let file_for_schema = file.try_clone()?;

    let reader = SerializedFileReader::new(file)?;
    let meta = reader.metadata();
    let row_count = meta.file_metadata().num_rows() as usize;

    let arrow_schema: SchemaRef = ParquetRecordBatchReaderBuilder::try_new(file_for_schema)?
        .schema()
        .clone();

    // FIX: use `::new`, not `.new`
    Ok(Box::new(ArrowDataFrame::new(
        path.to_string(),
        arrow_schema,
        row_count,
    )))
}

struct ArrowDataFrame {
    path: String,
    schema: SchemaRef,
    row_count: usize,
}

impl ArrowDataFrame {
    fn new(path: String, schema: SchemaRef, row_count: usize) -> Self {
        Self { path, schema, row_count }
    }
}

impl DataFrame for ArrowDataFrame {
    fn shape(&self) -> (usize, usize) {
        (self.row_count, self.schema.fields().len())
    }

    fn column_names(&self) -> Vec<String> {
        self.schema
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect()
    }

    fn column_types(&self) -> HashMap<String, String> {
        self.schema
            .fields()
            .iter()
            .map(|f| (f.name().to_string(), format!("{:?}", f.data_type())))
            .collect()
    }

    fn read_column_f64(&self, column_name: &str) -> Vec<f64> {
        let column_name_string = column_name.to_string();
        let file = File::open(&self.path)
            .unwrap_or_else(|e| panic!("failed to open parquet file '{}': {e}", self.path));
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap_or_else(|e| panic!("failed to build parquet reader: {e}"));
        let mut reader = builder.build()
            .unwrap_or_else(|e| panic!("failed to build record batch reader: {e}"));

        let mut values: Vec<f64> = Vec::with_capacity(self.row_count);

        while let Some(batch_res) = reader.next() {
            let batch = batch_res.unwrap_or_else(|e| panic!("error reading batch: {e}"));

            let idx = batch
                .schema()
                .index_of(&column_name_string)
                .unwrap_or_else(|_| panic!("column '{}' not found in batch", column_name_string));

            let col = batch
                .column(idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap_or_else(|| panic!("column '{}' is not Float64", column_name_string));

            // Map NULL -> NaN
            for opt in col.iter() {
                values.push(opt.unwrap_or(f64::NAN));
            }
        }

        values
    }
}

