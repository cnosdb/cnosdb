use std::fmt::Display;
use std::sync::Arc;

#[derive(Debug)]
pub struct DBResult {
    pub case_name: Arc<String>,
    pub request: String,
    pub response: String,
    pub is_sorted: bool,
    pub is_ok: bool,
    pub is_line_protocol: bool,
}

impl DBResult {
    pub fn parse(case_name: Arc<String>, lines: &str) -> Vec<DBResult> {
        let mut results = Vec::<DBResult>::new();

        let mut parsing = false;
        let mut parsing_header = true;
        let mut parsing_line_protocol = false;
        let mut req_buf = String::with_capacity(256);
        let mut resp_buf = String::with_capacity(256);
        let mut is_ok = true;
        let mut is_sorted = false;
        let mut is_line_protocol = false;

        for line in lines.lines() {
            if line.trim().is_empty() && parsing {
                results.push(DBResult {
                    case_name: case_name.clone(),
                    request: req_buf.trim_end().to_string(),
                    response: resp_buf.trim_end().to_string(),
                    is_ok,
                    is_sorted,
                    is_line_protocol,
                });

                parsing = false;
                parsing_header = true;
                parsing_line_protocol = false;
                req_buf.clear();
                resp_buf.clear();
                is_ok = true;
                is_sorted = false;
            } else if parsing_header {
                if line.starts_with("-- EXECUTE SQL:") {
                    parsing = true;
                    parsing_header = false;
                    if let Some(sql) = line.find(':').map(|p| line.split_at(p + 1).1.trim()) {
                        if let Some(q) = sql.find(" --").map(|p| sql.split_at(p).0.trim()) {
                            req_buf = q.to_string();
                        } else {
                            req_buf = sql.to_string();
                        }
                    }
                } else if line.starts_with("-- WRITE LINE PROTOCOL") {
                    parsing = true;
                    parsing_header = false;
                    parsing_line_protocol = true;
                    is_line_protocol = true;
                }
            } else if parsing_line_protocol {
                if line.starts_with("-- LINE PROTOCOL END") {
                    parsing_line_protocol = false;
                } else {
                    req_buf.push_str(line);
                    req_buf.push('\n');
                }
            } else if line.starts_with("-- AFTER_SORT") {
                is_sorted = true;
            } else if line.starts_with("-- ERROR:") {
                is_ok = false;
            } else {
                resp_buf.push_str(line);
                resp_buf.push('\n');
            }
        }

        results
    }
}

impl Display for DBResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_line_protocol {
            writeln!(f, "-- WRITE LINE PROTOCOL --")?;
            writeln!(f, "{}", self.request)?;
            writeln!(f, "-- LINE PROTOCOL END --")?;
        } else {
            writeln!(f, "-- EXECUTE SQL: {} --", self.request)?;
            if self.is_sorted {
                writeln!(f, "-- AFTER_SORT --")?;
            }
        }

        if self.is_ok {
            writeln!(f, "{}\n", self.response)?;
        } else {
            writeln!(f, "-- ERROR: --")?;
            writeln!(f, "{}\n", self.response)?;
        }
        Ok(())
    }
}
