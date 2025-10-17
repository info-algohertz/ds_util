use crate::dataframe::DataFrame;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};

const SEPARATOR: char = ',';

pub fn read_csv(
    path: &str,
    column_names: Option<Vec<String>>,
) -> Result<Box<dyn DataFrame>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let mut lines = reader.lines();

    // Determine headers
    let mut headers: Vec<String> = if let Some(mut cols) = column_names {
        // Ensure each column has a name; fill empties and make unique
        normalize_headers(&mut cols);
        cols
    } else {
        // Take first line as header
        let first = lines.next().ok_or("CSV is empty; cannot read header")??;
        let mut cols: Vec<String> = split_csv_line(&first);
        normalize_headers(&mut cols);
        cols
    };

    // Prepare storage
    let mut column_data: HashMap<String, Vec<String>> = HashMap::new();
    for h in &headers {
        column_data.insert(h.clone(), Vec::new());
    }

    // Read all rows as strings
    for line_res in lines {
        let line = line_res?;
        // Skip empty lines (common in CSVs)
        if line.trim().is_empty() {
            continue;
        }
        let mut fields = split_csv_line(&line);

        // Pad / truncate to match header count
        if fields.len() < headers.len() {
            fields.resize(headers.len(), String::new());
        } else if fields.len() > headers.len() {
            // If there are extra fields, extend headers with generated names
            let extra = fields.len() - headers.len();
            for _ in 0..extra {
                headers.push(format!("col{}", headers.len() + 1));
            }
            normalize_headers(&mut headers);
            // Ensure storage has new columns
            for h in &headers {
                column_data.entry(h.clone()).or_insert_with(Vec::new);
            }
        }

        // Append to per-column vectors
        for (i, value) in fields.into_iter().enumerate() {
            let h = &headers[i];
            column_data.get_mut(h).unwrap().push(value);
        }
    }

    Ok(Box::new(CsvDataFrame {
        column_order: headers,
        column_data,
    }))
}

struct CsvDataFrame {
    column_order: Vec<String>,
    column_data: HashMap<String, Vec<String>>,
}

impl DataFrame for CsvDataFrame {
    fn shape(&self) -> (usize, usize) {
        // rows = max column length (CSV may be ragged); cols = number of columns
        let rows = self
            .column_data
            .values()
            .map(|v| v.len())
            .max()
            .unwrap_or(0);
        (rows, self.column_order.len())
    }

    fn column_names(&self) -> Vec<String> {
        self.column_order.clone()
    }

    fn column_types(&self) -> HashMap<String, String> {
        // By design: we store everything as strings initially.
        let mut m = HashMap::new();
        for c in &self.column_order {
            m.insert(c.clone(), "string".to_string());
        }
        m
    }

    fn read_column_i64(&self, column_name: &str) -> Vec<i64> {
        match self.column_data.get(column_name) {
            Some(col) => col.iter().map(|s| parse_i64_lossy(s)).collect(),
            None => panic!("Column '{}' not found", column_name),
        }
    }

    fn read_column_f64(&self, column_name: &str) -> Vec<f64> {
        match self.column_data.get(column_name) {
            Some(col) => col.iter().map(|s| parse_f64_lossy(s)).collect(),
            None => panic!("Column '{}' not found", column_name),
        }
    }

    fn read_index_microsecond(&self) -> Vec<i64> {
        // Heuristic: prefer "timestamp" then "index", otherwise first column.
        if self.column_order.is_empty() {
            return Vec::new();
        }
        let candidate = if self.column_data.contains_key("timestamp") {
            "timestamp"
        } else if self.column_data.contains_key("index") {
            "index"
        } else {
            &self.column_order[0]
        };
        self.read_column_i64(candidate)
    }
}

/* -------------------- helpers -------------------- */

fn normalize_headers(headers: &mut Vec<String>) {
    // Replace empty with generated names and ensure uniqueness
    for (i, h) in headers.iter_mut().enumerate() {
        if h.trim().is_empty() {
            *h = format!("col{}", i + 1);
        }
    }
    // De-duplicate by suffixing _2, _3, ...
    let mut seen: HashSet<String> = HashSet::new();
    for i in 0..headers.len() {
        let original = headers[i].clone();
        if seen.insert(original.clone()) {
            continue;
        }
        // Need to make unique
        let mut k = 2usize;
        loop {
            let candidate = format!("{}_{}", original, k);
            if seen.insert(candidate.clone()) {
                headers[i] = candidate;
                break;
            }
            k += 1;
        }
    }
}

fn split_csv_line(line: &str) -> Vec<String> {
    // Minimal CSV split: split by SEPARATOR; trim surrounding whitespace.
    // (If you need quoted-field semantics, swap this for the `csv` crate.)
    line.split(SEPARATOR)
        .map(|s| s.trim().to_string())
        .collect()
}

fn parse_i64_lossy(s: &str) -> i64 {
    // Accept blanks as 0; trim; allow underscores; strip quotes
    let t = s.trim().trim_matches('"').replace('_', "");
    if t.is_empty() {
        0
    } else {
        t.parse::<i64>().unwrap_or(0)
    }
}

fn parse_f64_lossy(s: &str) -> f64 {
    // Accept blanks as NaN; trim; allow commas in European style by replacing ',' with '.'
    // but ONLY if there is no thousands separator conflict. For simplicity, just try normal parse first.
    let t = s.trim().trim_matches('"');
    if t.is_empty() {
        f64::NAN
    } else {
        t.parse::<f64>().unwrap_or_else(|_| {
            // Try replacing commas with dots (e.g., "3,14")
            let alt = t.replace(',', ".");
            alt.parse::<f64>().unwrap_or(f64::NAN)
        })
    }
}
