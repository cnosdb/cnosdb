#![allow(dead_code)]
#![cfg(test)]
use crate::case::{CnosdbRequest, E2eExecutor, Step};
use crate::cluster_def;

#[test]
fn fault_migration() {
    let executor = E2eExecutor::new_cluster(
        "stream_computing",
        "fault_migration",
        cluster_def::three_meta_two_data_bundled(),
    );
    executor.execute_steps_customized(&[
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: "http://127.0.0.1:8902/api/v1/sql?tenant=cnosdb&db=public",
                sql: "create database test_stream_migrate with replica 2;",
                resp: Ok(vec![]),
                sorted: false,
                regex: false,
            },
            auth: None,
        },
        Step::Sleep(1),
        Step::CnosdbRequest {
            req: CnosdbRequest::Query { url: "http://127.0.0.1:8902/api/v1/sql?tenant=cnosdb&db=public", sql: "show databases;", resp: Ok(vec!["database_name", "cluster_schema", "public", "test_stream_migrate", "usage_schema"]), sorted: false, regex: false, },
            auth: None,
        },
        Step::Sleep(1),
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: "http://127.0.0.1:8902/api/v1/sql?tenant=cnosdb&db=test_stream_migrate",
                sql: "create table readings(latitude DOUBLE, longitude DOUBLE,elevation  DOUBLE,velocity DOUBLE,heading DOUBLE,grade DOUBLE,fuel_consumption  DOUBLE,load_capacity  DOUBLE,fuel_capacity DOUBLE,nominal_fuel_consumption DOUBLE, tags(device_version,driver,fleet,model,name));",
                resp: Ok(vec![]),
                sorted: false,
                regex: false, },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: "http://127.0.0.1:8902/api/v1/sql?tenant=cnosdb&db=test_stream_migrate",
                sql: "create stream table readings_stream(time timestamp, device_version string, driver string, fleet string, model string, name string, latitude DOUBLE, longitude DOUBLE,elevation  DOUBLE,velocity DOUBLE,heading DOUBLE,grade DOUBLE,fuel_consumption  DOUBLE,load_capacity  DOUBLE,fuel_capacity DOUBLE,nominal_fuel_consumption DOUBLE)with (db = 'test_stream_migrate', table = 'readings', event_time_column = 'time') engine = tskv;",
                resp: Ok(vec![]),
                sorted: false,
                regex: false, },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: "http://127.0.0.1:8902/api/v1/sql?tenant=cnosdb&db=test_stream_migrate",
                sql: "create table readings_down_sampling_1hour(max_latitude double, avg_longitude DOUBLE, sum_elevation  DOUBLE, count_velocity bigint, mean_heading DOUBLE, median_grade DOUBLE, min_fuel_consumption  DOUBLE, first_load_capacity  DOUBLE, last_fuel_capacity DOUBLE,mode_nominal_fuel_consumption DOUBLE, tags(device_version,driver,fleet,model,name));",
                resp: Ok(vec![]),
                sorted: false,
                regex: false, },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: "http://127.0.0.1:8902/api/v1/sql?tenant=cnosdb&db=test_stream_migrate",
                sql: "INSERT INTO readings_down_sampling_1hour (time, device_version, driver, fleet, model
	, name, max_latitude, avg_longitude, sum_elevation, count_velocity
	, mean_heading, median_grade, min_fuel_consumption, first_load_capacity, last_fuel_capacity
	, mode_nominal_fuel_consumption)
SELECT date_bin(INTERVAL '1' HOUR, time) AS time, device_version, driver
	, fleet, model, name, max(latitude)
	, avg(longitude), sum(elevation)
	, count(velocity), mean(heading)
	, median(grade), min(fuel_consumption)
	, first(time, load_capacity)
	, last(time, fuel_capacity)
	, mode(nominal_fuel_consumption)
FROM readings_stream
GROUP BY date_bin(INTERVAL '1' HOUR, time), device_version, driver, fleet, model, name;",
                resp: Ok(vec!["query_id", r"\d+"]),
                sorted: false,
                regex: true, },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: "http://127.0.0.1:8902/api/v1/sql?tenant=cnosdb&db=test_stream_migrate",
                sql: "show queries;",
                resp: Ok(vec!["query_id,query_type,query_text,user_name,tenant_name,database_name,state,duration",
                "\\d+,stream,\\\"INSERT INTO readings_down_sampling_1hour (time, device_version, driver, fleet, model\\n\\t, name, max_latitude, avg_longitude, sum_elevation, count_velocity\\n\\t, mean_heading, median_grade, min_fuel_consumption, first_load_capacity, last_fuel_capacity\\n\\t, mode_nominal_fuel_consumption)\\nSELECT date_bin(INTERVAL '1' HOUR, time) AS time, device_version, driver\\n\\t, fleet, model, name, max(latitude)\\n\\t, avg(longitude), sum(elevation)\\n\\t, count(velocity), mean(heading)\\n\\t, median(grade), min(fuel_consumption)\\n\\t, first(time, load_capacity)\\n\\t, last(time, fuel_capacity)\\n\\t, mode(nominal_fuel_consumption)\\nFROM readings_stream\\nGROUP BY date_bin(INTERVAL '1' HOUR, time), device_version, driver, fleet, model, name;\\\",root,cnosdb,test_stream_migrate,SCHEDULING,\\d+\\.\\d+",
                "\\d+,batch,show queries;,root,cnosdb,test_stream_migrate,OPTMIZING,\\d+\\.\\d+"
                    ]),
                sorted: true,
                regex: true, },
            auth: None,
        },
        Step::StopDataNode(0),
        Step::Sleep(30),
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: "http://127.0.0.1:8912/api/v1/sql?tenant=cnosdb&db=test_stream_migrate",
                sql: "show queries;",
                resp: Ok(vec!["query_id,query_type,query_text,user_name,tenant_name,database_name,state,duration",
                "\\d+,stream,\\\"INSERT INTO readings_down_sampling_1hour (time, device_version, driver, fleet, model\\n\\t, name, max_latitude, avg_longitude, sum_elevation, count_velocity\\n\\t, mean_heading, median_grade, min_fuel_consumption, first_load_capacity, last_fuel_capacity\\n\\t, mode_nominal_fuel_consumption)\\nSELECT date_bin(INTERVAL '1' HOUR, time) AS time, device_version, driver\\n\\t, fleet, model, name, max(latitude)\\n\\t, avg(longitude), sum(elevation)\\n\\t, count(velocity), mean(heading)\\n\\t, median(grade), min(fuel_consumption)\\n\\t, first(time, load_capacity)\\n\\t, last(time, fuel_capacity)\\n\\t, mode(nominal_fuel_consumption)\\nFROM readings_stream\\nGROUP BY date_bin(INTERVAL '1' HOUR, time), device_version, driver, fleet, model, name;\\\",root,cnosdb,test_stream_migrate,SCHEDULING,\\d+\\.\\d+",
                "\\d+,batch,show queries;,root,cnosdb,test_stream_migrate,OPTMIZING,\\d+\\.\\d+"]),
                sorted: false,
                regex: true, },
            auth: None,
        },
    ],
        vec![
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
    ],
     vec![]
    );
}

#[test]
fn stream_computing() {
    let url = "http://127.0.0.1:8902/api/v1/sql?tenant=cnosdb&db=test_stream_computing";
    let executor = E2eExecutor::new_cluster(
        "stream_computing",
        "stream_computing",
        cluster_def::three_meta_three_data(),
    );
    executor.execute_steps_customized(&[
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url,
                sql: "create database test_stream_computing with replica 3;",
                resp: Ok(vec![]),
                sorted: false,
                regex: false,
            },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Query { url, sql: "show databases;", resp: Ok(vec!["database_name", "cluster_schema", "public", "test_stream_computing", "usage_schema"]), sorted: false, regex: false, },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url,
                sql: "create table readings(latitude DOUBLE, longitude DOUBLE,elevation  DOUBLE,velocity DOUBLE,heading DOUBLE,grade DOUBLE,fuel_consumption  DOUBLE,load_capacity  DOUBLE,fuel_capacity DOUBLE,nominal_fuel_consumption DOUBLE, tags(device_version,driver,fleet,model,name));",
                resp: Ok(vec![]),
                sorted: false,
                regex: false, },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url,
                sql: "create stream table readings_stream(time timestamp, device_version string, driver string, fleet string, model string, name string, latitude DOUBLE, longitude DOUBLE,elevation  DOUBLE,velocity DOUBLE,heading DOUBLE,grade DOUBLE,fuel_consumption  DOUBLE,load_capacity  DOUBLE,fuel_capacity DOUBLE,nominal_fuel_consumption DOUBLE)with (db = 'test_stream_computing', table = 'readings', event_time_column = 'time') engine = tskv;",
                resp: Ok(vec![]),
                sorted: false,
                regex: false, },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url,
                sql: "create table readings_down_sampling_1hour(max_latitude double, avg_longitude DOUBLE, sum_elevation  DOUBLE, count_velocity bigint, mean_heading DOUBLE, median_grade DOUBLE, min_fuel_consumption  DOUBLE, first_load_capacity  DOUBLE, last_fuel_capacity DOUBLE,mode_nominal_fuel_consumption DOUBLE, tags(device_version,driver,fleet,model,name));",
                resp: Ok(vec![]),
                sorted: false,
                regex: false, },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url,
                sql: "INSERT INTO readings_down_sampling_1hour (time, device_version, driver, fleet, model
	, name, max_latitude, avg_longitude, sum_elevation, count_velocity
	, mean_heading, median_grade, min_fuel_consumption, first_load_capacity, last_fuel_capacity
	, mode_nominal_fuel_consumption)
SELECT date_bin(INTERVAL '1' HOUR, time) AS time, device_version, driver
	, fleet, model, name, max(latitude)
	, avg(longitude), sum(elevation)
	, count(velocity), mean(heading)
	, median(grade), min(fuel_consumption)
	, first(time, load_capacity)
	, last(time, fuel_capacity)
	, mode(nominal_fuel_consumption)
FROM readings_stream
GROUP BY date_bin(INTERVAL '1' HOUR, time), device_version, driver, fleet, model, name;",
                resp: Ok(vec!["query_id", r"\d+"]),
                sorted: false,
                regex: true, },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Insert {
                url,
                sql: "insert into readings(time, device_version, driver, fleet, model, name, latitude, longitude, elevation, velocity, heading, grade, fuel_consumption, load_capacity, fuel_capacity, nominal_fuel_consumption) values('2021-01-01 00:00:00', 'v1', 'Driver', 'fl', 'm1', 'n1', 0.1, 0.38, 100.0, 100, 90, 0.1, 100, 100, 0.1, 3.14);",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::Sleep(6),
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url,
                sql: "select * from readings_down_sampling_1hour;",
                resp: Ok(vec!["time,device_version,driver,fleet,model,name,max_latitude,avg_longitude,sum_elevation,count_velocity,mean_heading,median_grade,min_fuel_consumption,first_load_capacity,last_fuel_capacity,mode_nominal_fuel_consumption", "2021-01-01T00:00:00.000000000,v1,Driver,fl,m1,n1,0.1,0.38,100.0,1,90.0,0.1,100.0,100.0,0.1,3.14"]),
                sorted: false,
                regex: true, },
            auth: None,
        },
    ],
        vec![
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
    ],
     vec![]
    );
}
