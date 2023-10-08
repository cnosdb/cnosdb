#[cfg(test)]
pub mod test {
    use std::process::Command;
    use std::sync::{Arc, Mutex};
    use std::thread;

    use http_protocol::status_code;
    use rand::seq::SliceRandom;
    use rand::Rng;

    use crate::utils::{clean_env, start_cluster, Client};

    fn move_vnode() {
        let mut rng = rand::thread_rng();
        let mut target_node = [1001, 2001][rng.gen_range(0..2)];
        let res = String::from_utf8_lossy(
            &Command::new("ls")
                .arg(format!("/tmp/cnosdb/{target_node}/db/data/cnosdb.public"))
                .output()
                .unwrap()
                .stdout,
        )
        .to_string();
        let vnodes = res
            .split_whitespace()
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();
        if vnodes.is_empty() {
            return;
        }
        target_node = if target_node == 1001 { 2001 } else { 1001 };
        let target_vnode = vnodes[rng.gen_range(0..vnodes.len())];
        if target_vnode.parse::<i32>().is_err() {
            return;
        }
        let client = Client::new("root".to_string(), Some(String::new()));
        client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                &format!("move vnode {target_vnode} to node {target_node}"),
            )
            .unwrap();
    }

    fn alter_replica() {
        let mut rng = rand::thread_rng();
        let target_replica = rng.gen_range(1..=2);
        let client = Client::new("root".to_string(), Some(String::new()));
        client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                &format!("alter database public set replica {target_replica}"),
            )
            .unwrap();
    }

    fn alter_shard() {
        let mut rng = rand::thread_rng();
        let target_shard = rng.gen_range(1..=10);
        let client = Client::new("root".to_string(), Some(String::new()));
        client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                &format!("alter database public set shard {target_shard}"),
            )
            .unwrap();
    }

    fn alter_vnode_duration() {
        let mut rng = rand::thread_rng();
        let target_vnode_duration = rng.gen_range(1..=365);
        let client = Client::new("root".to_string(), Some(String::new()));
        client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                &format!("alter database public set vnode_duration '{target_vnode_duration}d'"),
            )
            .unwrap();
    }

    #[test]
    fn case1() {
        println!("Test begin 'chaos_test_case_1'");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(4)
            .build()
            .unwrap();
        let runtime = Arc::new(runtime);

        clean_env();

        let (_meta, data) = start_cluster(runtime);

        let data = Arc::new(Mutex::new(data));
        let data_c = Arc::clone(&data);

        let count = Arc::new(Mutex::new(0));
        let count_c = Arc::clone(&count);

        let finished = Arc::new(Mutex::new(false));
        let finished_c1 = Arc::clone(&finished);
        let finished_c2 = Arc::clone(&finished);
        let finished_c3 = Arc::clone(&finished);

        let write_data = move || {
            let client = Client::new("root".to_string(), Some(String::new()));
            let mut count_g = count_c.lock().unwrap();
            while !*finished_c1.lock().unwrap() {
                let timestamp = 43200 * 1e9 as u64 * *count_g;
                client
                    .post(
                        "http://127.0.0.1:8902/api/v1/write?db=public",
                        &format!("ma,ta=a fa={} {}", count_g, timestamp),
                    )
                    .unwrap();
                *count_g += 1;
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        };

        let perform_actions = move || {
            let ops: Vec<(fn(), &'static str)> = vec![
                (move_vnode, "move_vnode"),
                (alter_replica, "alter_replica"),
                (alter_shard, "alter_shard"),
                (alter_vnode_duration, "alter_vnode_duration"),
            ];
            let mut rng = rand::thread_rng();
            let mut handles = vec![];
            let mut turn = 1;
            while !*finished_c2.lock().unwrap() {
                let mut ops_now = ops.clone();
                ops_now.shuffle(&mut rng);
                let ops_now = &ops_now[0..rng.gen_range(1..=ops.len())];
                let ops_str = ops_now
                    .iter()
                    .map(|f| f.1)
                    .fold(format!("turn{turn} actions: "), |acc, x| acc + x + ", ");
                println!("{}", &ops_str[..ops_str.len() - 2]);
                for op in ops_now {
                    let op = op.0.clone();
                    let t = thread::spawn(op);
                    handles.push(t);
                }
                turn += 1;
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
            for handle in handles {
                handle.join().unwrap();
            }
        };

        let t_restart = thread::spawn(move || {
            while !*finished_c3.lock().unwrap() {
                data_c.lock().unwrap().restart("config_8912.toml");
                std::thread::sleep(std::time::Duration::from_secs(5));
            }
        });
        let t_write_data = thread::spawn(write_data);
        let t_perform_action = thread::spawn(perform_actions);
        std::thread::sleep(std::time::Duration::from_secs(120));
        *finished.lock().unwrap() = true;
        t_restart.join().unwrap();
        t_write_data.join().unwrap();
        t_perform_action.join().unwrap();

        let resp = data
            .lock()
            .unwrap()
            .client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "select count(*) from ma",
            )
            .unwrap();
        assert_eq!(resp.status(), status_code::OK);
        let actual = resp.text().unwrap();
        let expected = format!("{}", *count.lock().unwrap());
        println!("actual: {}", actual);
        println!("expected: {}", expected);
        assert!(actual.contains(&expected));

        clean_env();
        println!("Test complete chaos_test_case_1");
    }
}
