use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::time::Instant;

use prettydiff::diff_lines;
use tokio::fs;
use walkdir::WalkDir;

use crate::error::{Error, Result};
use crate::query::*;
use crate::CLIENT;

#[derive(Clone, Debug)]
pub struct Case {
    name: String,
    path: PathBuf,
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
        Ok(Case { name, path })
    }

    pub fn case_name(&self) -> &str {
        &self.name
    }

    fn sql_file(&self) -> PathBuf {
        let mut res = self.path.to_owned();
        res.push(&self.name);
        res.set_extension("sql");
        res
    }

    fn out_file(&self) -> PathBuf {
        let mut res = self.path.to_owned();
        res.push(&self.name);
        res.set_extension("out");
        res
    }

    fn res_file(&self) -> PathBuf {
        let mut res = self.path.to_owned();
        res.push(&self.name);
        res.set_extension("result");
        res
    }

    pub async fn get_queries(&self) -> Result<Vec<Query>> {
        let sqls = fs::read_to_string(self.sql_file()).await?;
        Query::parse_queries(&sqls)
    }
    /// check out and expected result
    pub async fn check(&self, result: &str, out: &str) -> bool {
        let diff = diff_lines(result, out)
            .set_diff_only(true)
            .set_show_lines(true);

        let is_diff = diff
            .diff()
            .iter()
            .any(|diff| !matches!(diff, prettydiff::basic::DiffOp::Equal(_)));

        if is_diff {
            diff.prettytable();
            fs::write(self.out_file(), &out).await.unwrap();
        }
        !is_diff
    }

    /// run and return is succeed
    pub async fn run(&self) -> Result<()> {
        let queries = self.get_queries().await?;
        if queries.is_empty() {
            return Ok(());
        }

        println!("\t{} begin.", &self);
        let before = Instant::now();

        let result = fs::read_to_string(self.res_file()).await?;
        let out = CLIENT.execute_queries(&queries).await;
        let succeed = self.check(&result, &out).await;

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
        write!(
            f,
            "Case: {} at {}",
            &self.name,
            &self.path.as_os_str().to_str().unwrap()
        )
    }
}

/// search all cases
pub fn search_cases(path: &PathBuf) -> Result<Vec<Case>> {
    let mut sql_files: Vec<PathBuf> = Vec::new();
    let mut result_files: Vec<PathBuf> = Vec::new();

    for entry in WalkDir::new(path) {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            continue;
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
