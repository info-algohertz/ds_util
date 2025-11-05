use crate::dataframe::DataFrame;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};

const SEPARATOR: char = ',';

pub fn read_csv<S: Into<String>>(
    path: &str,
    column_names: Option<Vec<S>>,
) -> Result<Box<dyn DataFrame>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    // --- derive headers ---
    let mut headers: Vec<String> = if let Some(cols) = column_names {
        // Convert &str or String into owned Strings
        let mut h: Vec<String> = cols.into_iter().map(Into::into).collect();

        // Ensure non-empty
        for (i, name) in h.iter_mut().enumerate() {
            if name.trim().is_empty() {
                *name = format!("col{}", i + 1);
            }
        }

        // Ensure uniqueness by suffixing duplicates
        let mut seen = HashSet::new();
        for i in 0..h.len() {
            let base = h[i].clone();
            if seen.insert(base.clone()) {
                continue;
            }
            let mut k = 2usize;
            loop {
                let cand = format!("{}_{}", base, k);
                if seen.insert(cand.clone()) {
                    h[i] = cand;
                    break;
                }
                k += 1;
            }
        }
        h
    } else {
        // Read the first line as header
        let first_line = lines.next().ok_or("CSV is empty; cannot read header")??;
        let mut h: Vec<String> = first_line
            .split(SEPARATOR)
            .map(|s| s.trim().to_string())
            .collect();

        // Ensure non-empty and unique
        for (i, name) in h.iter_mut().enumerate() {
            if name.trim().is_empty() {
                *name = format!("col{}", i + 1);
            }
        }
        let mut seen = HashSet::new();
        for i in 0..h.len() {
            let base = h[i].clone();
            if seen.insert(base.clone()) {
                continue;
            }
            let mut k = 2usize;
            loop {
                let cand = format!("{}_{}", base, k);
                if seen.insert(cand.clone()) {
                    h[i] = cand;
                    break;
                }
                k += 1;
            }
        }
        h
    };

    // --- storage per column (all strings initially) ---
    let mut column_data: HashMap<String, Vec<String>> =
        headers.iter().cloned().map(|h| (h, Vec::new())).collect();

    // --- read remaining lines ---
    for line_res in lines {
        let line = line_res?;
        if line.trim().is_empty() {
            continue;
        }

        let mut fields: Vec<String> = line
            .split(SEPARATOR)
            .map(|s| s.trim().to_string())
            .collect();

        // Pad or extend if needed
        if fields.len() < headers.len() {
            fields.resize(headers.len(), String::new());
        } else if fields.len() > headers.len() {
            let extra = fields.len() - headers.len();
            for _ in 0..extra {
                headers.push(format!("col{}", headers.len() + 1));
            }
            // Ensure column_data has entries for any new headers
            for h in headers.iter().skip(column_data.len()) {
                column_data.entry(h.clone()).or_insert_with(Vec::new);
            }
        }

        // Append per-column values
        for (i, val) in fields.into_iter().enumerate() {
            let h = &headers[i];
            column_data.get_mut(h).unwrap().push(val);
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

    fn read_column_string(&self, column_name: &str) -> Vec<String> {
        match self.column_data.get(column_name) {
            Some(col) => col.to_vec(),
            None => panic!("Column '{}' not found", column_name),
        }
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
