use crate::case::step::{ControlStep, RequestStep, Sql, SqlNoResult};
use crate::cluster_def;
use crate::utils::global::E2eContext;

const SQL_CREATE_TABLE_1: &str = r#"create table readings(
  latitude DOUBLE, longitude DOUBLE, elevation DOUBLE, velocity DOUBLE, heading DOUBLE, grade DOUBLE,
  fuel_consumption DOUBLE, load_capacity DOUBLE, fuel_capacity DOUBLE, nominal_fuel_consumption DOUBLE,
  tags(device_version, driver, fleet, model, name)
);"#;

const SQL_CREATE_STREAM_TABLE_1: &str = r#"create stream table readings_stream(
  time timestamp, device_version string, driver string, fleet string, model string, name string,
  latitude DOUBLE, longitude DOUBLE, elevation DOUBLE, velocity DOUBLE, heading DOUBLE, grade DOUBLE,
  fuel_consumption DOUBLE, load_capacity DOUBLE, fuel_capacity DOUBLE, nominal_fuel_consumption DOUBLE
) with (db = 'test_stream_migrate', table = 'readings', event_time_column = 'time') engine = tskv;"#;

const SQL_CREATE_DOWN_SAMPLING_TABLE_1: &str = "create table readings_down_sampling_1hour(
  max_latitude double, avg_longitude DOUBLE, sum_elevation DOUBLE, count_velocity bigint, mean_heading DOUBLE, median_grade DOUBLE,
  min_fuel_consumption DOUBLE, first_load_capacity DOUBLE, last_fuel_capacity DOUBLE, mode_nominal_fuel_consumption DOUBLE,
  tags(device_version,driver,fleet,model,name)
);";

const SQL_INSERT_DOWN_SAMPLING_TABLE_1: &str = r#"INSERT INTO readings_down_sampling_1hour (time, device_version, driver, fleet, model, name, max_latitude, avg_longitude, sum_elevation, count_velocity, mean_heading, median_grade, min_fuel_consumption, first_load_capacity, last_fuel_capacity, mode_nominal_fuel_consumption)
  SELECT date_bin(INTERVAL '1' HOUR, time) AS time, device_version, driver, fleet, model, name, max(latitude), avg(longitude), sum(elevation), count(velocity), mean(heading), median(grade), min(fuel_consumption), first(time, load_capacity), last(time, fuel_capacity), mode(nominal_fuel_consumption)
  FROM readings_stream
  GROUP BY date_bin(INTERVAL '1' HOUR, time), device_version, driver, fleet, model, name;"#;

const SQL_SHOW_QUERIES_STREAM_TABLE_1: &str = r#"(\d+,batch,show queries;,root,cnosdb,test_stream_migrate,OPTMIZING,\d+\.\d+)|(\d+,stream,\"INSERT INTO readings_down_sampling_1hour \(time, device_version, driver, fleet, model, name, max_latitude, avg_longitude, sum_elevation, count_velocity, mean_heading, median_grade, min_fuel_consumption, first_load_capacity, last_fuel_capacity, mode_nominal_fuel_consumption\)
  SELECT date_bin\(INTERVAL '1' HOUR, time\) AS time, device_version, driver, fleet, model, name, max\(latitude\), avg\(longitude\), sum\(elevation\), count\(velocity\), mean\(heading\), median\(grade\), min\(fuel_consumption\), first\(time, load_capacity\), last\(time, fuel_capacity\), mode\(nominal_fuel_consumption\)
  FROM readings_stream
  GROUP BY date_bin\(INTERVAL '1' HOUR, time\), device_version, driver, fleet, model, name;\",root,cnosdb,test_stream_migrate,SCHEDULING,\d+\.\d+)"#;

#[test]
fn fault_migration() {
    let mut ctx = E2eContext::new("stream_computing", "fault_migration");
    let mut executor = ctx.build_executor(cluster_def::three_meta_two_data_bundled());
    let host_port_1 = executor.cluster_definition().data_cluster_def[0].http_host_port;
    let host_port_2 = executor.cluster_definition().data_cluster_def[1].http_host_port;

    executor.set_update_meta_config_fn_vec(vec![
        Some(Box::new(|c| {
            c.sys_config.system_database_replica = 2;
            c.heartbeat.heartbeat_recheck_interval = 10;
            c.heartbeat.heartbeat_expired_interval = 30;
        })),
        Some(Box::new(|c| {
            c.sys_config.system_database_replica = 2;
            c.heartbeat.heartbeat_recheck_interval = 10;
            c.heartbeat.heartbeat_expired_interval = 30;
        })),
    ]);

    executor.execute_steps(&[
        RequestStep::new_boxed(
            "create database",
            SqlNoResult::build_request_with_str(
                format!("http://{host_port_1}/api/v1/sql?tenant=cnosdb&db=public"),
                "create database test_stream_migrate with replica 2;",
                Ok(()),
            ),
            None, None,
        ),
        ControlStep::new_boxed_sleep("sleep 1s", 1),
        RequestStep::new_boxed(
            "show databases",
            Sql::build_request_with_str(
                format!("http://{host_port_1}/api/v1/sql?tenant=cnosdb&db=public"),
                "show databases;",
                Ok(vec!["database_name", "cluster_schema", "public", "test_stream_migrate", "usage_schema"]),
                false, false,
            ), None, None,
        ),
        ControlStep::new_boxed_sleep("sleep 1s", 1),
        RequestStep::new_boxed(
            "create table",
            SqlNoResult::build_request_with_str(
                format!("http://{host_port_1}/api/v1/sql?tenant=cnosdb&db=test_stream_migrate"),
                SQL_CREATE_TABLE_1,
                Ok(()),
            ),
            None, None,
        ),
        RequestStep::new_boxed(
            "create stream table",
            SqlNoResult::build_request_with_str(
                format!("http://{host_port_1}/api/v1/sql?tenant=cnosdb&db=test_stream_migrate"),
                SQL_CREATE_STREAM_TABLE_1,
                Ok(()),
            ),
            None, None,
        ),
        RequestStep::new_boxed(
            "create down-sampling table",
            SqlNoResult::build_request_with_str(
                format!("http://{host_port_1}/api/v1/sql?tenant=cnosdb&db=test_stream_migrate"),
                SQL_CREATE_DOWN_SAMPLING_TABLE_1,
                Ok(()),
            ),
            None, None,
        ),
        RequestStep::new_boxed(
            "insert to down-sampling table",
            Sql::build_request_with_str(
                format!("http://{host_port_1}/api/v1/sql?tenant=cnosdb&db=test_stream_migrate"),
                SQL_INSERT_DOWN_SAMPLING_TABLE_1,
                Ok(vec!["query_id", r"\d+"]),
                false, true,
            ),
            None, None,
        ),
        RequestStep::new_boxed(
            "show queries",
            Sql::build_request_with_str(
                format!("http://{host_port_1}/api/v1/sql?tenant=cnosdb&db=test_stream_migrate"),
                "show queries;",
                Ok(vec![
                    "query_id,query_type,query_text,user_name,tenant_name,database_name,state,duration",
                    SQL_SHOW_QUERIES_STREAM_TABLE_1,
                    SQL_SHOW_QUERIES_STREAM_TABLE_1,
                ]), false, true,
            ), None, None,
        ),
        ControlStep::new_boxed_stop_data_node("stop data node 0", 0),
        ControlStep::new_boxed_sleep("sleep 26s", 26),
        RequestStep::new_boxed(
            "show queries",
            Sql::build_request_with_str(
                format!("http://{host_port_2}/api/v1/sql?tenant=cnosdb&db=test_stream_migrate"),
                "show queries;",
                Ok(vec![
                        "query_id,query_type,query_text,user_name,tenant_name,database_name,state,duration",
                        SQL_SHOW_QUERIES_STREAM_TABLE_1,
                        SQL_SHOW_QUERIES_STREAM_TABLE_1,
                    ]), false, true,
            ), None, None,
        ),
    ],
    );
}

const SQL_CREATE_TABLE_2: &str = r#"create table readings(
  latitude DOUBLE, longitude DOUBLE, elevation DOUBLE, velocity DOUBLE, heading DOUBLE, grade DOUBLE,
  fuel_consumption DOUBLE, load_capacity DOUBLE, fuel_capacity DOUBLE, nominal_fuel_consumption DOUBLE,
  tags(device_version,driver,fleet,model,name)
);"#;

const SQL_CREATE_STREAM_TABLE_2: &str = r#"create stream table readings_stream(
  time timestamp, device_version string, driver string, fleet string, model string, name string,
  latitude DOUBLE, longitude DOUBLE, elevation DOUBLE, velocity DOUBLE, heading DOUBLE, grade DOUBLE,
  fuel_consumption DOUBLE, load_capacity DOUBLE, fuel_capacity DOUBLE, nominal_fuel_consumption DOUBLE
) with (db = 'test_stream_computing', table = 'readings', event_time_column = 'time') engine = tskv;"#;

const SQL_INSERT_DATA_2: &str = r#"insert into readings
  (time, device_version, driver, fleet, model, name, latitude, longitude, elevation, velocity, heading, grade, fuel_consumption, load_capacity, fuel_capacity, nominal_fuel_consumption)
values
  ('2021-01-01 00:00:00', 'v1', 'Driver', 'fl', 'm1', 'n1', 0.1, 0.38, 100.0, 100, 90, 0.1, 100, 100, 0.1, 3.14);"#;

const SQL_INSERT_DOWN_SAMPLING_TABLE_2: &str = r#"INSERT INTO readings_down_sampling_1hour (time, device_version, driver, fleet, model, name, max_latitude, avg_longitude, sum_elevation, count_velocity, mean_heading, median_grade, min_fuel_consumption, first_load_capacity, last_fuel_capacity, mode_nominal_fuel_consumption)
  SELECT date_bin(INTERVAL '1' HOUR, time) AS time, device_version, driver, fleet, model, name, max(latitude), avg(longitude), sum(elevation), count(velocity), mean(heading), median(grade), min(fuel_consumption), first(time, load_capacity), last(time, fuel_capacity), mode(nominal_fuel_consumption)
  FROM readings_stream
  GROUP BY date_bin(INTERVAL '1' HOUR, time), device_version, driver, fleet, model, name;"#;

#[test]
fn stream_computing() {
    let mut ctx = E2eContext::new("stream_computing", "stream_computing");
    let mut executor = ctx.build_executor(cluster_def::three_meta_three_data());
    let host_port = executor.cluster_definition().data_cluster_def[0].http_host_port;

    executor.set_update_meta_config_fn_vec(vec![
        Some(Box::new(|c| {
            c.sys_config.system_database_replica = 3;
            c.heartbeat.heartbeat_recheck_interval = 10;
            c.heartbeat.heartbeat_expired_interval = 30;
        })),
        Some(Box::new(|c| {
            c.sys_config.system_database_replica = 3;
            c.heartbeat.heartbeat_recheck_interval = 10;
            c.heartbeat.heartbeat_expired_interval = 30;
        })),
        Some(Box::new(|c| {
            c.sys_config.system_database_replica = 3;
            c.heartbeat.heartbeat_recheck_interval = 10;
            c.heartbeat.heartbeat_expired_interval = 30;
        })),
    ]);

    let url = &format!("http://{host_port}/api/v1/sql?tenant=cnosdb&db=test_stream_computing");

    executor.execute_steps(&[
        RequestStep::new_boxed(
            "create database",
            SqlNoResult::build_request_with_str(
                url,
                "create database test_stream_computing with replica 3;",
                Ok(()),
            ), None, None,
        ),
        RequestStep::new_boxed(
            "show databases",
            Sql::build_request_with_str(
                url,
                "show databases;",
                Ok(vec!["database_name", "cluster_schema", "public", "test_stream_computing", "usage_schema"]),
                false, false,
            ), None, None,
        ),
        RequestStep::new_boxed(
            "create table",
            SqlNoResult::build_request_with_str(
                url,
                SQL_CREATE_TABLE_2,
                Ok(()),
            ),
            None, None,
        ),
        RequestStep::new_boxed(
            "create stream table",
            SqlNoResult::build_request_with_str(
                url,
                SQL_CREATE_STREAM_TABLE_2,
                Ok(()),
            ),
            None, None,
        ),
        RequestStep::new_boxed(
            "create down-sampling table",
            SqlNoResult::build_request_with_str(
                url,
                "create table readings_down_sampling_1hour(max_latitude double, avg_longitude DOUBLE, sum_elevation  DOUBLE, count_velocity bigint, mean_heading DOUBLE, median_grade DOUBLE, min_fuel_consumption  DOUBLE, first_load_capacity  DOUBLE, last_fuel_capacity DOUBLE,mode_nominal_fuel_consumption DOUBLE, tags(device_version,driver,fleet,model,name));",
                Ok(()),
            ),
            None, None,
        ),
        RequestStep::new_boxed(
            "insert to down-sampling table",
            Sql::build_request_with_str(
                url,
                SQL_INSERT_DOWN_SAMPLING_TABLE_2,
                Ok(vec!["query_id", r"\d+"]),
                false, true,
            ),
            None, None,
        ),
        RequestStep::new_boxed(
            "insert data",
            SqlNoResult::build_request_with_str(
                url,
                SQL_INSERT_DATA_2,
                Ok(()),
            ), None, None,
        ),
        ControlStep::new_boxed_sleep("sleep 6s", 6),
        RequestStep::new_boxed(
            "select down-sampled data",
            Sql::build_request_with_str(
                url,
                "select * from readings_down_sampling_1hour;",
                Ok(vec![
                    "time,device_version,driver,fleet,model,name,max_latitude,avg_longitude,sum_elevation,count_velocity,mean_heading,median_grade,min_fuel_consumption,first_load_capacity,last_fuel_capacity,mode_nominal_fuel_consumption",
                    "2021-01-01T00:00:00.000000000,v1,Driver,fl,m1,n1,0.1,0.38,100.0,1,90.0,0.1,100.0,100.0,0.1,3.14"
                ]), false, true,
            ), None, None,
        ),
        ControlStep::new_boxed_sleep("sleep 6s", 6),
        RequestStep::new_boxed(
            "select down-sampled data",
            Sql::build_request_with_str(
                url,
                "select count(*) from readings_down_sampling_1hour;",
                Ok(vec![r"COUNT\(U?Int\d{1,2}\(\d+\)\)", "1"]),
                false, true,
            ), None, None,
        ),
        ControlStep::new_boxed_sleep("sleep 6s", 6),
        RequestStep::new_boxed(
            "select down-sampled data",
            Sql::build_request_with_str(
                url,
                "select count(*) from readings_down_sampling_1hour;",
                Ok(vec![r"COUNT\(U?Int\d{1,2}\(\d+\)\)", "1"]),
                false, true,
            ), None, None,
        ),
    ]);
}
