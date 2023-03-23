use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use tokio::fs;
use walkdir::WalkDir;

use crate::db_request::DBRequest;
use crate::db_result::DBResult;
use crate::error::{Error, Result};
use crate::CLIENT;

#[derive(Clone, Debug)]
pub struct Case {
    name: Arc<String>,
    path: PathBuf,
    sql_file_path: PathBuf,
    res_file_path: PathBuf,
    out_file_path: PathBuf,
}

impl Case {
    pub fn new(sql_path: PathBuf) -> Result<Case> {
        let name = sql_path
            .file_stem()
            .ok_or(Error::CaseNew)?
            .to_str()
            .ok_or(Error::CaseNew)?
            .to_string();

        let path = sql_path.parent().ok_or(Error::CaseNew)?.to_path_buf();
        let sql_file_path = Self::sql_file(&path, &name);
        let res_file_path = Self::res_file(&path, &name);
        let out_file_path = Self::out_file(&path, &name);
        Ok(Case {
            name: Arc::new(name),
            path,
            sql_file_path,
            res_file_path,
            out_file_path,
        })
    }

    pub fn case_name(&self) -> &str {
        self.name.as_str()
    }

    fn sql_file(path: impl AsRef<Path>, name: &str) -> PathBuf {
        let mut path: PathBuf = path.as_ref().into();
        path.push(name);
        path.set_extension("sql");
        path
    }

    fn out_file(path: impl AsRef<Path>, name: &str) -> PathBuf {
        let mut path: PathBuf = path.as_ref().into();
        path.push(name);
        path.set_extension("out");
        path
    }

    fn res_file(path: impl AsRef<Path>, name: &str) -> PathBuf {
        let mut path: PathBuf = path.as_ref().into();
        path.push(name);
        path.set_extension("result");
        path
    }

    pub async fn get_queries(&self) -> Result<Vec<DBRequest>> {
        let content = fs::read_to_string(&self.sql_file_path).await?;
        Ok(DBRequest::parse_requests(&content))
    }

    pub async fn get_results(&self) -> Result<Vec<DBResult>> {
        let content = fs::read_to_string(&self.res_file_path).await?;
        Ok(DBResult::parse(self.name.clone(), &content))
    }

    /// check out and expected result
    pub async fn check(&self, origins: &[DBResult], outs: &[DBResult]) -> bool {
        if origins.len() != outs.len() {
            let out_put = outs
                .iter()
                .map(|r| r.to_string())
                .collect::<Vec<String>>()
                .join("\n");

            fs::write(&self.out_file_path, out_put.as_bytes())
                .await
                .unwrap();
            return false;
        }

        let mut no_diff = true;
        let mut diff_buf = String::with_capacity(512);
        for (i, (l, r)) in origins.iter().zip(outs.iter()).enumerate() {
            diff_buf.clear();
            if check_diff(&l.response, &r.response, &mut diff_buf) {
                no_diff = false;
                println!(
                    r#"====================
Case: {} [{}]
--------------------
{}
--------------------
{}
===================="#,
                    &l.case_name,
                    i + 1,
                    &l.request,
                    &diff_buf.trim_end()
                );
            }
        }

        if !no_diff {
            let out_put = outs
                .iter()
                .map(|r| r.to_string())
                .collect::<Vec<String>>()
                .join("\n");

            fs::write(&self.out_file_path, out_put.as_bytes())
                .await
                .expect("write out_file_path error");
        }

        no_diff
    }

    /// run and return is succeed
    pub async fn run(&self) -> Result<()> {
        let queries = self.get_queries().await?;
        if queries.is_empty() {
            return Ok(());
        }
        let results = self.get_results().await?;
        if results.len() != queries.len() {
            println!(
                "Warn: queries({}) and results({}) are not the same length.",
                queries.len(),
                results.len()
            );
        }

        println!("\t{} begin.", &self);
        let before = Instant::now();

        let outs = CLIENT.execute_db_request(self.case_name(), &queries).await;
        let outs = DBResult::parse(self.name.clone(), &outs);
        if outs.len() != queries.len() {
            println!(
                "Warn: queries({}) and outs({}) are not the same length.",
                queries.len(),
                outs.len()
            );
        }

        let succeed = self.check(&results, &outs).await;

        let after = Instant::now();

        let millis = after.duration_since(before).as_millis();

        if succeed {
            println!("\t{} succeed! in {} ms", self, millis);
            Ok(())
        } else {
            println!("\t{} FAIL! in {} ms", self, millis);
            Err(Error::CaseFail)
        }
    }
}

impl Display for Case {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Case: {} at {}", &self.name, &self.path.display())
    }
}

/// search all cases
pub fn search_cases(path: &PathBuf, pattern: Option<String>) -> Result<Vec<Case>> {
    let mut sql_files: Vec<PathBuf> = Vec::new();
    let mut result_files: Vec<PathBuf> = Vec::new();

    for entry in WalkDir::new(path) {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            continue;
        }
        if let Some(ref pat) = pattern {
            if path.file_stem().ok_or(Error::CaseSearch)?.ne(pat.as_str()) {
                continue;
            }
        }

        if path.extension().ok_or(Error::CaseSearch)?.eq("sql") {
            sql_files.push(PathBuf::from(&path));
            continue;
        }

        if path.extension().ok_or(Error::CaseSearch)?.eq("result") {
            result_files.push(PathBuf::from(&path));
            continue;
        }
    }

    sql_files.sort();
    result_files.sort();

    for i in 0..sql_files.len() {
        if !sql_files[i].parent().eq(&result_files[i].parent()) {
            return Err(Error::CaseNotMatch);
        }

        if !sql_files[i].file_stem().eq(&result_files[i].file_stem()) {
            return Err(Error::CaseNotMatch);
        }
    }

    let mut res = Vec::new();
    for sql_file in sql_files {
        res.push(Case::new(sql_file)?);
    }
    Ok(res)
}

fn check_diff(str1: &str, str2: &str, buf: &mut String) -> bool {
    let (mut ll, mut rl) = (1, 1);
    let mut is_diff = false;
    for diff in diff::lines(str1, str2) {
        match diff {
            diff::Result::Left(l) => {
                is_diff = true;
                buf.push_str(format!("{:<4}|    | -{}\n", ll, l).as_str());
                ll += 1;
            }
            diff::Result::Right(r) => {
                is_diff = true;
                buf.push_str(format!("    |{:<4}| +{}\n", rl, r).as_str());
                rl += 1;
            }
            _ => {
                ll += 1;
                rl += 1;
            }
        }
    }
    is_diff
}

#[test]
fn test_check_diff() {
    let str1 = r#"200 OK
time,t0,f0,t1,f1
1970-01-01T00:00:00.000000001,2,4,3,
1970-01-01T00:00:00.000000005,6,8,7,
1970-01-01T00:00:00.000000009,10,12,11,13
1970-01-01T00:00:00.000000014,15,17,16,18
"#;

    let str2 = r#"200 OK
time,t0,f0,t1,f1
1970-01-01T00:00:00.000000009,10,12,11,13
1970-01-01T00:00:00.000000014,15,17,16,18
1970-01-01T00:00:00.000000001,2,4,3,
1970-01-01T00:00:00.000000005,6,8,7,
"#;

    let mut diff_buf = String::with_capacity(512);
    assert!(check_diff(str1, str2, &mut diff_buf));
    assert_eq!(
        diff_buf,
        r#"3   |    | -1970-01-01T00:00:00.000000001,2,4,3,
4   |    | -1970-01-01T00:00:00.000000005,6,8,7,
    |5   | +1970-01-01T00:00:00.000000001,2,4,3,
    |6   | +1970-01-01T00:00:00.000000005,6,8,7,
"#
    )
}
