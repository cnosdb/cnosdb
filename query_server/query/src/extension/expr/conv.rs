fn conv(sequence1: Vec<f64>, sequence2: Vec<f64>) -> Vec<f64> {
  let len1 = sequence1.len();
  let len2 = sequence2.len();
  let mut result = vec![0.0; len1 + len2 - 1];
  for i in 0..len1 {
      for j in 0..len2 {
          if sequence1[i].is_nan() || sequence2[j].is_nan() {
              continue;
          }
          result[i + j] += sequence1[i] * sequence2[j];
      }
  }
  result.retain(|&x| x != 0.0);
  result
}