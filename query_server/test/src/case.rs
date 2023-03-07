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
        let mut all_diff_outs = vec![];
        for (i, (l, r)) in origins.iter().zip(outs.iter()).enumerate() {
            let (mut ll, mut rl) = (1, 1);
            let mut is_diff = false;
            diff_buf.clear();
            for diff in diff::lines(&l.response, &r.response) {
                match diff {
                    diff::Result::Left(l) => {
                        is_diff = true;
                        diff_buf.push_str(format!("{:<4}|    | -{}\n", ll, l).as_str());
                        ll += 1;
                    }
                    diff::Result::Right(r) => {
                        is_diff = true;
                        diff_buf.push_str(format!("    |{:<4}| +{}\n", rl, r).as_str());
                        rl += 1;
                    }
                    _ => {
                        ll += 1;
                        rl += 1;
                    }
                }
            }
            if is_diff {
                no_diff = false;
                all_diff_outs.push(i);
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
        if !all_diff_outs.is_empty() {
            diff_buf.clear();
            for i in all_diff_outs {
                diff_buf.push_str("====================\n");
                diff_buf.push_str(format!("Query [{}]\n--------------------\n", i + 1).as_str());
                diff_buf.push_str(format!("{}", outs[i]).as_str());
            }
            diff_buf.push_str("====================");
            fs::write(&self.out_file_path, diff_buf).await.unwrap();
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
