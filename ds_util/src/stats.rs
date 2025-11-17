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

pub fn get_mean(x: &Vec<f64>) -> f64 {
    // Filter out NaN values
    let valid_values: Vec<f64> = x.iter().copied().filter(|v| !v.is_nan()).collect();

    let n = valid_values.len();

    // Return NaN if no valid values
    if n == 0 {
        return f64::NAN;
    }

    // Use Welford's online algorithm for numerical stability
    // This avoids overflow and reduces rounding errors
    let mut mean = 0.0;
    for (i, &value) in valid_values.iter().enumerate() {
        let count = (i + 1) as f64;
        // Incremental mean update: mean_new = mean_old + (value - mean_old) / count
        mean += (value - mean) / count;
    }

    mean
}

pub fn get_corr(x: &Vec<f64>, y: &Vec<f64>) -> f64 {
    // Filter out pairs where either value is NaN
    let valid_pairs: Vec<(f64, f64)> = x
        .iter()
        .zip(y.iter())
        .filter(|(xi, yi)| !xi.is_nan() && !yi.is_nan())
        .map(|(xi, yi)| (*xi, *yi))
        .collect();

    let n = valid_pairs.len();

    // Need at least 2 points for correlation
    if n < 2 {
        return f64::NAN;
    }

    // Calculate means using Welford's online algorithm for numerical stability
    let (mean_x, mean_y) = {
        let mut mx = 0.0;
        let mut my = 0.0;
        for (i, &(xi, yi)) in valid_pairs.iter().enumerate() {
            let count = (i + 1) as f64;
            mx += (xi - mx) / count;
            my += (yi - my) / count;
        }
        (mx, my)
    };

    // Calculate correlation using the numerically stable two-pass algorithm
    let mut sum_xy = 0.0;
    let mut sum_xx = 0.0;
    let mut sum_yy = 0.0;

    for &(xi, yi) in &valid_pairs {
        let dx = xi - mean_x;
        let dy = yi - mean_y;
        sum_xy += dx * dy;
        sum_xx += dx * dx;
        sum_yy += dy * dy;
    }

    // Avoid division by zero
    if sum_xx == 0.0 || sum_yy == 0.0 {
        return f64::NAN;
    }

    let correlation = sum_xy / (sum_xx * sum_yy).sqrt();

    // Clamp to [-1, 1] to handle numerical errors
    correlation.clamp(-1.0, 1.0)
}

