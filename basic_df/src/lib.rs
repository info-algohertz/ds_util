use std::collections::HashMap;
use std::fs::File;

use arrow::datatypes::SchemaRef;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};

pub trait DataFrame: Send + Sync {
    fn shape(&self) -> (usize, usize);
    fn column_names(&self) -> Vec<String>;
    fn column_types(&self) -> HashMap<String, String>;
}

pub fn read_parquet(path: &str) -> Result<Box<dyn DataFrame>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let file_for_schema = file.try_clone()?;

    let reader = SerializedFileReader::new(file)?;
    let meta = reader.metadata();
    let total_rows = meta.file_metadata().num_rows() as usize;

    let arrow_schema: SchemaRef = ParquetRecordBatchReaderBuilder::try_new(file_for_schema)?
        .schema()
        .clone();

    Ok(Box::new(ArrowDataFrame::new(arrow_schema, total_rows)))
}

struct ArrowDataFrame {
    schema: SchemaRef,
    rows: usize,
}

impl ArrowDataFrame {
    fn new(schema: SchemaRef, rows: usize) -> Self {
        Self { schema, rows }
    }
}

impl DataFrame for ArrowDataFrame {
    fn shape(&self) -> (usize, usize) {
        (self.rows, self.schema.fields().len())
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
}

