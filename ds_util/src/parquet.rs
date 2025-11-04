use std::collections::HashMap;
use std::fs::File;

use arrow::array::{Array, Float64Array};
use arrow::datatypes::SchemaRef;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};

use crate::dataframe::DataFrame;

const INDEX_NAME: &str = "__index_level_0__";

pub fn read_parquet(path: &str) -> Result<Box<dyn DataFrame>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let file_for_schema = file.try_clone()?;

    let reader = SerializedFileReader::new(file)?;
    let meta = reader.metadata();
    let row_count = meta.file_metadata().num_rows() as usize;

    let arrow_schema: SchemaRef = ParquetRecordBatchReaderBuilder::try_new(file_for_schema)?
        .schema()
        .clone();

    Ok(Box::new(ArrowDataFrame {
        path: path.to_string(),
        schema: arrow_schema,
        row_count,
    }))
}

struct ArrowDataFrame {
    path: String,
    schema: SchemaRef,
    row_count: usize,
}

impl DataFrame for ArrowDataFrame {
    fn shape(&self) -> (usize, usize) {
        (self.row_count, self.schema.fields().len())
    }

    fn column_names(&self) -> Vec<String> {
        self.schema
            .fields()
            .iter()
            .filter(|f| f.name() != INDEX_NAME)
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

    fn read_column_string(&self, _column_name: &str) -> Vec<String> {
        panic!("Not implemented.");
    }

    fn read_column_i64(&self, _column_name: &str) -> Vec<i64> {
        panic!("Not implemented.");
    }

    fn read_column_f64(&self, column_name: &str) -> Vec<f64> {
        let file = File::open(&self.path)
            .unwrap_or_else(|e| panic!("failed to open parquet file '{}': {e}", self.path));
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap_or_else(|e| panic!("failed to build parquet reader: {e}"));
        let mut reader = builder
            .build()
            .unwrap_or_else(|e| panic!("failed to build record batch reader: {e}"));

        let mut values: Vec<f64> = Vec::with_capacity(self.row_count);

        while let Some(batch_res) = reader.next() {
            let batch = batch_res.unwrap_or_else(|e| panic!("error reading batch: {e}"));

            let idx = batch
                .schema()
                .index_of(column_name)
                .unwrap_or_else(|_| panic!("column '{}' not found in batch", column_name));

            let col = batch
                .column(idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap_or_else(|| panic!("column '{}' is not Float64", column_name));

            for opt in col.iter() {
                values.push(opt.unwrap_or(f64::NAN));
            }
        }

        values
    }

    fn read_index_microsecond(&self) -> Vec<i64> {
        use arrow::array::TimestampMicrosecondArray;
        use arrow::datatypes::{DataType, TimeUnit};

        let file = File::open(&self.path)
            .unwrap_or_else(|e| panic!("failed to open parquet file '{}': {e}", self.path));

        // No need for `mut` here either
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap_or_else(|e| panic!("failed to build parquet reader: {e}"));
        let mut reader = builder
            .build()
            .unwrap_or_else(|e| panic!("failed to build record batch reader: {e}"));

        let mut values: Vec<i64> = Vec::with_capacity(self.row_count);
        let mut global_row: usize = 0;

        while let Some(batch_res) = reader.next() {
            let batch = batch_res.unwrap_or_else(|e| panic!("error reading batch: {e}"));

            // FIX: hold the schema in a local binding so the &Field borrow is valid
            let schema = batch.schema();
            let idx = schema
                .index_of(INDEX_NAME)
                .unwrap_or_else(|_| panic!("index column '{}' not found", INDEX_NAME));

            // Validate dtype once per batch
            let field = schema.field(idx);
            match field.data_type() {
                DataType::Timestamp(TimeUnit::Microsecond, tz) if tz.as_deref() == Some("UTC") => {}
                other => panic!(
                    "index column '{}' has dtype {:?}, expected Timestamp(Microsecond, Some(\"UTC\"))",
                    INDEX_NAME, other
                ),
            }

            let col = batch
                .column(idx)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap_or_else(|| {
                    panic!(
                        "index column '{}' is not a TimestampMicrosecondArray",
                        INDEX_NAME
                    )
                });

            for (j, opt) in col.iter().enumerate() {
                let ts = opt.unwrap_or_else(|| {
                    panic!(
                        "index column '{}' contains a NULL at row {}",
                        INDEX_NAME,
                        global_row + j
                    )
                });
                values.push(ts);
            }

            global_row += col.len();
        }

        values
    }
}
