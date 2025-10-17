pub fn get_percentiles(values: &[f64], percentiles: &[f64]) -> Vec<f64> {
    // Filter out NaN values first
    let mut filtered: Vec<f64> = values.iter().copied().filter(|v| !v.is_nan()).collect();

    if filtered.is_empty() {
        // Return f64::NAN for all requested percentiles
        return vec![f64::NAN; percentiles.len()];
    }

    filtered.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = filtered.len();

    percentiles
        .iter()
        .map(|&p| {
            assert!(
                (0.0..=100.0).contains(&p),
                "Percentile must be between 0 and 100."
            );

            if n == 1 {
                return filtered[0];
            }

            let rank = p / 100.0 * (n as f64 - 1.0);
            let lower = rank.floor() as usize;
            let upper = rank.ceil() as usize;

            if lower == upper {
                filtered[lower]
            } else {
                let weight = rank - lower as f64;
                filtered[lower] * (1.0 - weight) + filtered[upper] * weight
            }
        })
        .collect()
}

pub fn get_percentile(values: &[f64], percentile: f64) -> f64 {
    get_percentiles(values, &[percentile])[0]
}
