use std::collections::HashMap;
use std::fs::File;

use arrow::datatypes::SchemaRef;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};

pub trait DataFrame: Send + Sync {
    /// (rows, cols)
    fn shape(&self) -> (usize, usize);
    /// Column names in order
    fn column_names(&self) -> Vec<String>;
    /// Column types as debug strings (Arrow dtypes) by name
    fn column_types(&self) -> HashMap<String, String>;
}

/// Metadata-only constructor from Parquet:
/// - Row count from Parquet footer (sum of row-group rows)
/// - Arrow schema via reader builder (reads schema, not data pages)
pub fn read_parquet(path: &str) -> Result<Box<dyn DataFrame>, Box<dyn std::error::Error>> {
    // 1) Row count from footer metadata (no data pages)
    let file = File::open(path)?;
    let reader = SerializedFileReader::new(file)?;
    let meta = reader.metadata();
    let total_rows: usize = meta
        .row_groups()
        .iter()
        .map(|rg| rg.num_rows() as usize)
        .sum();

    // 2) Arrow schema (no data pages)
    //    Using a fresh File; builder only inspects schema/metadata.
    let arrow_schema: SchemaRef = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?
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

