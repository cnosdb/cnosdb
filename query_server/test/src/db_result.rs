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
    pub is_open_tsdb: bool,
    pub is_open_tsdb_json: bool,
}

impl DBResult {
    pub fn parse(case_name: Arc<String>, lines: &str) -> Vec<DBResult> {
        let mut results = Vec::<DBResult>::new();

        let mut parsing = false;
        let mut parsing_header = true;
        let mut parsing_line_protocol = false;
        let mut parsing_opentsdb_protocol = false;
        let mut parsing_opentsdb_json = false;
        let mut req_buf = String::with_capacity(256);
        let mut resp_buf = String::with_capacity(256);
        let mut is_ok = true;
        let mut is_sorted = false;
        let mut is_line_protocol = false;
        let mut is_open_tsdb = false;
        let mut is_open_tsdb_json = false;

        for line in lines.lines() {
            if line.trim().is_empty() && parsing {
                results.push(DBResult {
                    case_name: case_name.clone(),
                    request: req_buf.trim_end().to_string(),
                    response: resp_buf.trim_end().to_string(),
                    is_ok,
                    is_sorted,
                    is_line_protocol,
                    is_open_tsdb,
                    is_open_tsdb_json,
                });

                parsing = false;
                parsing_header = true;
                parsing_line_protocol = false;
                parsing_opentsdb_protocol = false;
                parsing_opentsdb_json = false;
                req_buf.clear();
                resp_buf.clear();
                is_ok = true;
                is_sorted = false;
                is_line_protocol = false;
                is_open_tsdb = false;
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
                    parsing_opentsdb_protocol = false;
                    is_line_protocol = true;
                    is_open_tsdb = false;
                } else if line.starts_with("-- WRITE OPEN TSDB PROTOCOL ") {
                    parsing = true;
                    parsing_header = false;
                    parsing_line_protocol = false;
                    parsing_opentsdb_protocol = true;
                    is_line_protocol = false;
                    is_open_tsdb = true;
                } else if line.starts_with("-- WRITE OPEN TSDB JSON") {
                    parsing = true;
                    parsing_header = false;
                    parsing_line_protocol = false;
                    parsing_opentsdb_protocol = false;
                    parsing_opentsdb_json = true;
                    is_line_protocol = false;
                    is_open_tsdb = false;
                    is_open_tsdb_json = true;
                }
            } else if parsing_line_protocol {
                if line.starts_with("-- LINE PROTOCOL END") {
                    parsing_line_protocol = false;
                } else {
                    req_buf.push_str(line);
                    req_buf.push('\n');
                }
            } else if parsing_opentsdb_protocol {
                if line.starts_with("-- OPEN TSDB PROTOCOL END") {
                    parsing_opentsdb_protocol = false;
                } else {
                    req_buf.push_str(line);
                    req_buf.push('\n');
                }
            } else if parsing_opentsdb_json {
                if line.starts_with("-- OPEN TSDB JSON END") {
                    parsing_opentsdb_json = false;
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
        if parsing {
            results.push(DBResult {
                case_name,
                request: req_buf.trim_end().to_string(),
                response: resp_buf.trim_end().to_string(),
                is_ok,
                is_sorted,
                is_line_protocol,
                is_open_tsdb,
                is_open_tsdb_json,
            });
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
        } else if self.is_open_tsdb {
            writeln!(f, "-- WRITE OPEN TSDB PROTOCOL --")?;
            writeln!(f, "{}", self.request)?;
            writeln!(f, "-- OPEN TSDB PROTOCOL END --")?;
        } else if self.is_open_tsdb_json {
            writeln!(f, "-- WRITE OPEN TSDB JSON --")?;
            writeln!(f, "{}", self.request)?;
            writeln!(f, "-- OPEN TSDB JSON END --")?;
        } else {
            writeln!(f, "-- EXECUTE SQL: {} --", self.request)?;
            if self.is_sorted {
                writeln!(f, "-- AFTER_SORT --")?;
            }
        }

        if self.is_ok {
            writeln!(f, "{}", self.response.trim())?;
            if self.response.ends_with("200 OK") {
                writeln!(f)?;
            }
        } else {
            writeln!(f, "{}\n-- ERROR:  --", self.response)?;
        }
        Ok(())
    }
}
