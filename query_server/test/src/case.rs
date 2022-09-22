use crate::client::CLIENT;
use crate::query::*;
use prettydiff::diff_lines;
use std::path::PathBuf;
use std::time::Instant;
use tokio::fs;
use walkdir::WalkDir;

/// one test case
#[derive(Clone, Debug)]
pub struct Case {
    case_name: String,
    sql_file: PathBuf,
    result_file: PathBuf,
    out_file: PathBuf,
    queries: Vec<Query>,
}

pub fn search_cases(path: &PathBuf) -> Vec<Case> {
    let mut sqls: Vec<PathBuf> = Vec::new();
    let mut results: Vec<PathBuf> = Vec::new();

    for entry in WalkDir::new(path) {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            continue;
        }

        if path.extension().unwrap().eq("sql") {
            sqls.push(PathBuf::from(&path));
            continue;
        }

        if path.extension().unwrap().eq("result") {
            results.push(PathBuf::from(&path));
            continue;
        }
    }
    sqls.sort();
    results.sort();
    let mut res = Vec::new();

    for i in 0..sqls.len() {
        assert_eq!(
            sqls[i].parent(),
            results[i].parent(),
            "sql must .sql and .result must one-to-one correspondence"
        );
        assert_eq!(
            sqls[i].file_stem(),
            results[i].file_stem(),
            "sql must .sql and .result must one-to-one correspondence"
        );

        res.push(Case::new(sqls[i].clone(), results[i].clone()));
    }
    res
}

impl Case {
    /// check out and expected result
    pub async fn check(&self, result: &str, out: &str) -> bool {
        let diff = diff_lines(&result, &out)
            .set_diff_only(true)
            .set_show_lines(true);

        let is_diff = diff
            .diff()
            .iter()
            .any(|diff| !matches!(diff, prettydiff::basic::DiffOp::Equal(_)));

        if is_diff {
            diff.prettytable();
            fs::write(&self.out_file, &out).await.unwrap();
        }
        !is_diff
    }

    /// run and return is succeed
    pub async fn run(&mut self) -> bool {
        self.load_queries().await;
        if self.queries.is_empty() {
            return true;
        }
        println!(
            "Case: {} in {} begin",
            self.case_name,
            self.sql_file.parent().unwrap().to_str().unwrap()
        );
        let before = Instant::now();

        let result = fs::read_to_string(&self.result_file).await.unwrap();
        let out = CLIENT.execute_queries(&self.queries).await;
        let succeed = self.check(&result, &out).await;

        let after = Instant::now();

        if succeed {
            println!(
                "Case: {} at {} succeed! in {}",
                self.case_name,
                self.sql_file.parent().unwrap().to_str().unwrap(),
                after.duration_since(before).as_millis()
            );
        } else {
            println!(
                "Case: {} at {} FAIL! in {} ms",
                self.case_name,
                self.sql_file.parent().unwrap().to_str().unwrap(),
                after.duration_since(before).as_millis()
            );
        }

        succeed
    }

    pub fn case_name(&self) -> &str {
        &self.case_name
    }

    pub fn new(sql_file: PathBuf, result_file: PathBuf) -> Case {
        let name = sql_file.file_stem().unwrap().to_str().unwrap().to_string();
        let mut out_file = PathBuf::from(sql_file.parent().unwrap());
        out_file.push(sql_file.file_stem().unwrap());
        out_file.set_extension("out");

        Case {
            case_name: name,
            sql_file,
            result_file,
            out_file,
            queries: Vec::new(),
        }
    }

    pub async fn load_queries(&mut self) {
        let input = fs::read_to_string(&self.sql_file).await.unwrap();
        Query::parse_queries(&input, &mut self.queries);
    }
}

#[test]
fn test_parse_queries() {
    Case::new(PathBuf::from("a"), PathBuf::from("b"));
}
