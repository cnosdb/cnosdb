use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use serde::Deserialize;
use tokio::sync::mpsc::channel;
use tokio::sync::oneshot;

use crate::case::{search_cases, Case};
use crate::error::Result;

/// test group
#[derive(Clone, Deserialize, Debug)]
pub struct Group {
    name: String,
    tests: Vec<String>,
    parallel: bool,

    #[serde(skip)]
    cases: Vec<Case>,
}

impl Group {
    pub fn contains_case(&self, case_name: &str) -> bool {
        self.tests.iter().any(|test| test.eq(case_name))
    }

    pub fn new_with_case(case: Case) -> Self {
        Group {
            name: case.case_name().to_string(),
            tests: vec![case.case_name().to_string()],
            parallel: false,
            cases: vec![case],
        }
    }

    pub fn add_case(&mut self, case: Case) {
        self.cases.push(case)
    }

    pub async fn run(&mut self) -> Vec<Case> {
        if self.parallel {
            self.run_parallel().await
        } else {
            self.run_serial().await
        }
    }

    // run in serial and return failed case num
    pub async fn run_serial(&self) -> Vec<Case> {
        let mut failed_cases = Vec::new();

        for case in &self.cases {
            if (case.run().await).is_err() {
                failed_cases.push(case.clone());
            }
        }

        failed_cases
    }

    /// run in parallel and return failed cases
    pub async fn run_parallel(&self) -> Vec<Case> {
        let (sender, mut receiver) = channel(64);
        let cases = self.cases.clone();

        let handler = tokio::spawn(async move {
            let mut handlers = Vec::new();
            for case in cases {
                let sender = sender.clone();
                let (tx, rx) = oneshot::channel::<Option<Case>>();
                let handler = tokio::spawn(async move {
                    sender.send((case.clone(), tx)).await.unwrap();
                    if let Ok(Some(case)) = rx.await {
                        Some(case)
                    } else {
                        None
                    }
                });
                handlers.push(handler);
            }
            let mut failed_cases = Vec::new();
            for handler in handlers {
                if let Some(case) = handler.await.unwrap() {
                    failed_cases.push(case);
                }
            }
            failed_cases
        });

        while let Some((case, tx)) = receiver.recv().await {
            tokio::spawn(async move {
                let result = case.run().await;
                match result {
                    Ok(_) => tx.send(None).unwrap(),

                    Err(e) => {
                        println!("\t{} {:#?}", &case, e);
                        tx.send(Some(case)).unwrap();
                    }
                }
            });
        }

        handler.await.unwrap()
    }
}

#[derive(Clone, Deserialize, Debug)]
pub struct TestGroups {
    groups: Vec<Group>,
}

impl TestGroups {
    pub fn load(path: &PathBuf, pattern: Option<String>) -> Result<Self> {
        let cases = search_cases(path, pattern)?;
        let toml_str = fs::read_to_string(path.join("TestGroups.toml")).unwrap();
        let mut res: TestGroups = toml::from_str(&toml_str).unwrap();
        res.load_cases(cases);
        Ok(res)
    }

    pub fn load_cases(&mut self, cases: Vec<Case>) {
        let mut is_in_group: Vec<bool> = cases.iter().map(|_| false).collect();

        for i in 0..cases.len() {
            let case = &cases[i];
            self.groups
                .iter_mut()
                .filter(|group| group.contains_case(case.case_name()))
                .for_each(|group| {
                    group.add_case(case.clone());
                    is_in_group[i] = true;
                })
        }

        for i in 0..cases.len() {
            if !is_in_group[i] {
                self.groups.push(Group::new_with_case(cases[i].clone()))
            }
        }
    }

    /// serial run group
    pub async fn run(&mut self) -> Vec<Case> {
        let mut failed_cases = Vec::new();
        self.groups.retain(|group| !group.cases.is_empty());

        let parallel_information = |group: &Group| {
            if group.parallel {
                "parallel"
            } else {
                "serial"
            }
        };
        let groups_num = self.groups.len();

        for (i, group) in self.groups.iter_mut().enumerate() {
            println!(
                "TestGroup {} is running by {} ({}/{})",
                group.name.as_str(),
                parallel_information(group),
                i + 1,
                groups_num
            );
            let before = Instant::now();
            let failed_num = failed_cases.len();
            failed_cases.extend(group.run().await.into_iter());
            let after = Instant::now();
            println!(
                "TestGroup {} finished in {} ms has {} FAIL\n",
                group.name.as_str(),
                after.duration_since(before).as_millis(),
                failed_num
            );
        }
        failed_cases
    }
}
