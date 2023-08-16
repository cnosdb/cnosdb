// This will wrap the logic for incrementing the sum for the third moment of a series of floats (i.e. Sum (i=1..N) of (i-avg)^3)
// Math is sourced from https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Higher-order_statistics
pub(crate) mod m3 {
    use trace::info;

    // Add a value x to the set.  n, sx, sxx, sx3 are the values from prior to including x.
    pub(crate) fn accum(n: f64, sx: f64, sxx: f64, sx3: f64, x: f64) -> f64 {
        info!("accum: {n}, {sx}, {sxx}, {sx3}, {x}");
        let delta = x - (sx / n);
        let n = n + 1.0;
        sx3 + delta.powi(3) * (n - 1.0) * (n - 2.0) / n.powi(2) - (3.0 * delta * sxx / n)
    }
    // Remove a value x from the set.  Here n, sx, sxx are all the values from the set after x has been removed.
    // old_sx3 is the current value prior to the remove (sx3 after the removal is the returned value)
    pub(crate) fn remove(new_n: f64, new_sx: f64, new_sxx: f64, old_sx3: f64, x: f64) -> f64 {
        let delta = x - (new_sx / new_n);
        let n = new_n + 1.0;
        old_sx3 - (delta.powi(3) * (n - 1.0) * (n - 2.0) / n.powi(2) - (3.0 * delta * new_sxx / n))
    }
    // Combine two sets a and b and returns the sx3 for the combined set.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn combine(
        na: f64,
        nb: f64,
        sxa: f64,
        sxb: f64,
        sxxa: f64,
        sxxb: f64,
        sx3a: f64,
        sx3b: f64,
    ) -> f64 {
        let nx = na + nb;
        let delta = sxb / nb - sxa / na;
        sx3a + sx3b
            + delta.powi(3) * na * nb * (na - nb) / nx.powi(2)
            + (na * sxxb - (nb * sxxa)) * 3.0 * delta / nx
    }
}

// This will wrap the logic for incrementing the sum for the fourth moment of a series of floats (i.e. Sum (i=1..N) of (i-avg)^4)
// Math is sourced from https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Higher-order_statistics
pub(crate) mod m4 {
    // Add a value x to the set.  n, sx, sxx, sx3, sx4 are the values from prior to including x.
    pub(crate) fn accum(n: f64, sx: f64, sxx: f64, sx3: f64, sx4: f64, x: f64) -> f64 {
        let delta = x - (sx / n);
        let n = n + 1.0;
        sx4 + delta.powi(4) * (n - 1.0) * (n.powi(2) - 3.0 * n + 3.0) / n.powi(3)
            + 6.0 * delta.powi(2) * sxx / n.powi(2)
            - 4.0 * delta * sx3 / n
    }
    // Remove a value x from the set.  Here n, sx, sxx, sx3 are all the values from the set after x has been removed.
    // old_sx4 is the current value prior to the remove (sx4 after the removal is the returned value)
    pub(crate) fn remove(
        new_n: f64,
        new_sx: f64,
        new_sxx: f64,
        new_sx3: f64,
        old_sx4: f64,
        x: f64,
    ) -> f64 {
        let delta = x - (new_sx / new_n);
        let n = new_n + 1.0;
        old_sx4
            - (delta.powi(4) * (n - 1.0) * (n.powi(2) - 3.0 * n + 3.0) / n.powi(3)
                + 6.0 * delta.powi(2) * new_sxx / n.powi(2)
                - 4.0 * delta * new_sx3 / n)
    }
    // Combine two sets a and b and returns the sx4 for the combined set.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn combine(
        na: f64,
        nb: f64,
        sxa: f64,
        sxb: f64,
        sxxa: f64,
        sxxb: f64,
        sx3a: f64,
        sx3b: f64,
        sx4a: f64,
        sx4b: f64,
    ) -> f64 {
        let nx = na + nb;
        let delta = sxb / nb - sxa / na;
        sx4a + sx4b
            + delta.powi(4) * na * nb * (na.powi(2) - na * nb + nb.powi(2)) / nx.powi(3)
            + 6.0 * (na.powi(2) * sxxb + nb.powi(2) * sxxa) * delta.powi(2) / nx.powi(2)
            + 4.0 * (na * sx3b - nb * sx3a) * delta / nx
    }
}

#[test]
fn test_m3_accum() {
    // 3, 3, 0, 0, 1
    let a = m3::accum(8., 11., 1.875, 0.4687499999999998, 2.);
    assert!(a.eq(&0.24691358024691334));
}
