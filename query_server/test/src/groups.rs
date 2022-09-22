use crate::case::{search_cases, Case};
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::oneshot;

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

    pub fn new_with_onecase(case: Case) -> Self {
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

    pub async fn run(&mut self) -> usize {
        if self.parallel {
            self.run_parallel().await
        } else {
            self.run_serial().await
        }
    }
    // run in serial and return failed case num
    pub async fn run_serial(&mut self) -> usize {
        let mut fail_num = 0;

        for case in &mut self.cases {
            if !case.run().await {
                fail_num += 1
            }
        }
        fail_num
    }
    // run in parallel and return failed case num
    pub async fn run_parallel(&mut self) -> usize {
        let (sender, mut receiver) = unbounded_channel();
        let cases = self.cases.clone();

        let handler = tokio::spawn(async move {
            let mut handlers = Vec::new();
            for case in cases {
                let sender = sender.clone();
                let (tx, rx) = oneshot::channel::<bool>();
                let handler = tokio::spawn(async move {
                    sender.send((case.clone(), tx)).unwrap();
                    if let Ok(succeed) = rx.await {
                        succeed
                    } else {
                        false
                    }
                });
                handlers.push(handler);
            }
            let mut fail_num = 0;
            for handler in handlers {
                if !handler.await.unwrap() {
                    fail_num += 1;
                }
            }
            fail_num
        });

        while let Some((mut case, tx)) = receiver.recv().await {
            tokio::spawn(async move {
                let succeed = case.run().await; // 10s
                tx.send(succeed).unwrap();
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
    pub fn load(path: &PathBuf) -> Self {
        let cases = search_cases(path);
        let toml_str = fs::read_to_string(path.join("TestGroups.toml")).unwrap();
        let mut res: TestGroups = toml::from_str(&toml_str).unwrap();
        res.load_cases(cases);
        res
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
                self.groups.push(Group::new_with_onecase(cases[i].clone()))
            }
        }
    }

    /// serial run group
    pub async fn run(&mut self) {
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
            let fail_num = group.run().await;
            let after = Instant::now();

            println!(
                "TestGroup {} finished in {} ms has {} FAIL",
                group.name.as_str(),
                after.duration_since(before).as_millis(),
                fail_num
            );
        }
    }
}
