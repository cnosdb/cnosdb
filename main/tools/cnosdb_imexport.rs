#![allow(dead_code)]
#![allow(unused)]

use chrono::prelude::*;
use clap::{command, Args, Parser, Subcommand};
use meta::store::key_path::{self, KeyPath};
use reqwest::header::{ACCEPT, AUTHORIZATION};
use serde::{Deserialize, Serialize};

#[derive(Debug, Parser)]
#[command(name = "cnosdb-imexport")]
#[command(about = "CnosDB command line tools; export/import/migrate cluster data")]
struct Cli {
    #[command(subcommand)]
    subcmd: CliCommand,
}

#[derive(Debug, Subcommand)]
enum CliCommand {
    /// Export meta & user data.
    Export(ExportArgs),

    /// Import meta & user data.
    Import(ImportArgs),

    /// Migrate data cross cluster.
    Migrate(MigrateArgs),
}

#[derive(Debug, Args)]
struct ExportArgs {
    /// user:password@ip:port
    #[arg(short, long)]
    src: String,

    /// data path
    #[arg(short, long)]
    path: String,

    #[arg(short, long, global = true)]
    conn: Option<String>,
}

#[derive(Debug, Args)]
struct ImportArgs {
    /// data path
    #[arg(short, long)]
    path: String,

    /// user:password@ip:port
    #[arg(short, long)]
    dst: String,

    #[arg(short, long, global = true)]
    conn: Option<String>,
}

#[derive(Debug, Args)]
struct MigrateArgs {
    /// user:password@ip:port
    #[arg(short, long)]
    src: String,

    /// user:password@ip:port
    #[arg(short, long)]
    dst: String,

    /// Staging area
    #[arg(short, long)]
    path: String,

    #[arg(short, long, global = true)]
    conn: Option<String>,
}

const META_FILENAME: &str = "./meta_data.src";
const SCHEMA_FILENAME: &str = "./schema_data.src";

// COPY INTO air FROM 'gcs://test/air/' CONNECTION = (gcs_base_url = 'http://localhost:4443',disable_oauth=true) FILE_FORMAT = (TYPE = 'CSV');
// COPY INTO 'gcs://test/air' FROM air CONNECTION = (gcs_base_url='http://localhost:4443',disable_oauth = true) FILE_FORMAT = (TYPE = 'CSV');
// COPY INTO 'file:///tmp/xxx' FROM table_name FILE_FORMAT = (TYPE = 'PARQUET');
// COPY INTO table_name FROM 'file:///tmp/xxx/' FILE_FORMAT = (TYPE = 'PARQUET', DELIMITER = ',');
// curl -XPOST http://127.0.0.1:8901/dump --o ./meta_dump.data
// curl -XPOST http://127.0.0.1:8901/restore --data-binary "@./meta_dump.data"
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    println!("# Input args: {:?}", cli);

    if let CliCommand::Export(args) = &cli.subcmd {
        println!("# Export  Data: {:?}", export(args).await);
    } else if let CliCommand::Import(args) = &cli.subcmd {
        println!("# Import  Data: {:?}", import(args).await);
    } else if let CliCommand::Migrate(args) = &cli.subcmd {
        println!("# Migrate Data: {:?}", migrate(args).await);
    }

    Ok(())
}

async fn export(args: &ExportArgs) -> Result<(), String> {
    let path = "/api/v1/meta_leader";
    let src_meta = cnosdb_http_client(&args.src, "".to_string(), path, None).await?;
    dump_meta(&src_meta, META_FILENAME).await?;
    dump_schema(&args.src, SCHEMA_FILENAME).await?;

    let content = std::fs::read_to_string(SCHEMA_FILENAME).map_err(|err| err.to_string())?;
    let lines: Vec<&str> = content.split('\n').collect();
    for line in lines {
        let strs: Vec<&str> = line.split('|').collect();
        if strs.len() != 3 {
            continue;
        }

        let (tenant, db, table) = (strs[0], strs[1], strs[2]);
        let time_str = Local::now().format("Now: %H:%M:%S").to_string();
        println!(
            "# Begin  export table({}.{}.{})  [{}]",
            tenant, db, table, time_str
        );

        let uri = format!("{}/{}", args.src, db);
        let location = format!("{}/{}_{}_{}", args.path, tenant, db, table);
        let rsp1 = dump_table(&uri, tenant, table, &location, args.conn.clone()).await?;
        println!(
            "# Export table({}.{}.{}) data: {}\n",
            tenant, db, table, rsp1
        );
    }

    Ok(())
}

async fn import(args: &ImportArgs) -> Result<(), String> {
    let path = "/api/v1/meta_leader";
    let dst_meta = cnosdb_http_client(&args.dst, "".to_string(), path, None).await?;
    let (data, count) = filter_meta_data(META_FILENAME)?;
    let rsp = restore_meta(&dst_meta, data).await?;
    if rsp.contains(&count.to_string()) {
        println!("# Import meta data success, total: {}\n", count);
    } else {
        return Err(format!(
            "migrate meta data filed, dump: {}, restore: {}",
            count, rsp
        ));
    }

    let content = std::fs::read_to_string(SCHEMA_FILENAME).map_err(|err| err.to_string())?;
    let lines: Vec<&str> = content.split('\n').collect();
    for line in lines {
        let strs: Vec<&str> = line.split('|').collect();
        if strs.len() != 3 {
            continue;
        }

        let (tenant, db, table) = (strs[0], strs[1], strs[2]);
        let time_str = Local::now().format("Now: %H:%M:%S").to_string();
        println!(
            "# Begin import table({}.{}.{})  [{}]",
            tenant, db, table, time_str
        );

        let uri = format!("{}/{}", args.dst, db);
        let location = format!("{}/{}_{}_{}", args.path, tenant, db, table);
        let rsp = restore_table(&uri, tenant, table, &location, args.conn.clone()).await?;
        println!(
            "# Import table({}.{}.{}) data: {}\n",
            tenant, db, table, rsp
        );
    }

    Ok(())
}

async fn migrate(args: &MigrateArgs) -> Result<(), String> {
    let path = "/api/v1/meta_leader";
    let src_meta = cnosdb_http_client(&args.src, "".to_string(), path, None).await?;
    let dst_meta = cnosdb_http_client(&args.dst, "".to_string(), path, None).await?;
    dump_meta(&src_meta, META_FILENAME).await?;
    let (data, count) = filter_meta_data(META_FILENAME)?;
    let rsp = restore_meta(&dst_meta, data).await?;
    if rsp.contains(&count.to_string()) {
        println!("# Migrate meta data success, total: {}\n", count);
    } else {
        return Err(format!(
            "migrate meta data filed, dump: {}, restore: {}",
            count, rsp
        ));
    }

    let tenants = list_tenants(&args.src).await?;
    for i in 0..tenants.len() {
        let tenant = tenants.get(i).unwrap();
        let databases = list_databases(&args.src, tenant).await?;
        for j in 0..databases.len() {
            let db = databases.get(j).unwrap();
            let tables = list_tables(&format!("{}/{}", args.src, db), tenant).await?;
            let end_bar = format!("{}.{}.{}", tenants.len(), databases.len(), tables.len());
            for k in 0..tables.len() {
                let table = tables.get(k).unwrap();

                let bar = format!("{}.{}.{}", i + 1, j + 1, k + 1);
                let time_str = Local::now().format("Now: %H:%M:%S").to_string();
                println!(
                    "# Begin  export table({}.{}.{}); ({} --> {}) [{}]",
                    tenant, db, table, bar, end_bar, time_str
                );

                let loc = format!("{}/{}_{}_{}", args.path, tenant, db, table);

                let uri = format!("{}/{}", args.src, db);
                let rsp1 = dump_table(&uri, tenant, table, &loc, args.conn.clone()).await?;
                println!("# Export table({}.{}.{}) data: {}", tenant, db, table, rsp1);

                let uri = format!("{}/{}", args.dst, db);
                let rsp2 = restore_table(&uri, tenant, table, &loc, args.conn.clone()).await?;
                println!(
                    "# Import table({}.{}.{}) data: {}\n",
                    tenant, db, table, rsp2
                );

                if rsp1 != rsp2 {
                    return Err(format!("copy table({}) not pass!", table));
                }
            }
        }
    }

    Ok(())
}

async fn dump_meta(addr: &str, path: &str) -> Result<(), String> {
    let client = reqwest::Client::new();
    let url = format!("http://{}/dump", addr);
    let rsp = client
        .post(&url)
        .send()
        .await
        .map_err(|err| err.to_string())?
        .text()
        .await
        .map_err(|err| err.to_string())?;

    std::fs::write(path, rsp).map_err(|err| err.to_string())?;

    Ok(())
}

async fn restore_meta(addr: &str, data: String) -> Result<String, String> {
    let client = reqwest::Client::new();
    let url = format!("http://{}/restore", addr);
    let rsp = client
        .post(&url)
        .body(data)
        .send()
        .await
        .map_err(|err| err.to_string())?
        .text()
        .await
        .map_err(|err| err.to_string())?;

    Ok(rsp)
}

fn filter_meta_data(src_path: &str) -> Result<(String, usize), String> {
    let content = std::fs::read_to_string(src_path).map_err(|err| err.to_string())?;

    let mut count = 0;
    let mut data = "".to_string();
    let lines: Vec<&str> = content.split('\n').collect();
    for line in lines {
        let kv: Vec<&str> = line.splitn(2, ": ").collect();
        if kv.len() != 2 {
            continue;
        }

        let strs: Vec<&str> = kv[0].split('/').collect();
        if kv[0] == KeyPath::already_init()
            || kv[0] == KeyPath::version()
            || (strs.len() == 3 && strs[2] == key_path::AUTO_INCR_ID)
            || (strs.len() == 3 && strs[2] == key_path::RESOURCE_INFOS_MARK)
            || (strs.len() == 4 && strs[2] == key_path::DATA_NODES)
            || (strs.len() == 4 && strs[2] == key_path::DATA_NODES_METRICS)
            || (strs.len() == 8 && strs[6] == key_path::BUCKETS)
        {
            continue;
        }

        count += 1;
        data += &(line.to_owned() + "\n");
    }

    Ok((data, count))
}

async fn dump_schema(uri: &str, path: &str) -> Result<(), String> {
    let mut data = "".to_string();
    let tenants = list_tenants(uri).await?;
    for tenant in tenants.iter() {
        let databases = list_databases(uri, tenant).await?;
        for db in databases.iter() {
            let tables = list_tables(&format!("{}/{}", uri, db), tenant).await?;
            for table in tables.iter() {
                data += &format!("{}|{}|{}\n", tenant, db, table);
            }
        }
    }

    std::fs::write(path, data).map_err(|err| err.to_string())?;

    Ok(())
}

async fn dump_table(
    uri: &str,
    tenant: &str,
    table: &str,
    location: &str,
    conn: Option<String>,
) -> Result<String, String> {
    let sql = if let Some(conn) = conn {
        format!(
            "COPY INTO '{}' FROM {} CONNECTION = ({}) FILE_FORMAT = (TYPE = 'PARQUET')",
            location, table, conn
        )
    } else {
        format!(
            "COPY INTO '{}' FROM {} FILE_FORMAT = (TYPE = 'PARQUET')",
            location, table
        )
    };

    let rsp = cnosdb_http_client(uri, sql, "/api/v1/sql", Some(tenant.to_string())).await?;

    Ok(rsp)
}

async fn restore_table(
    uri: &str,
    tenant: &str,
    table: &str,
    location: &str,
    conn: Option<String>,
) -> Result<String, String> {
    let sql = if let Some(conn) = conn {
        format!(
        "COPY INTO {} FROM '{}/' CONNECTION = ({}) FILE_FORMAT = (TYPE = 'PARQUET', DELIMITER = ',')",
        table, location,conn
    )
    } else {
        format!(
            "COPY INTO {} FROM '{}/' FILE_FORMAT = (TYPE = 'PARQUET', DELIMITER = ',')",
            table, location
        )
    };

    let rsp = cnosdb_http_client(uri, sql, "/api/v1/sql", Some(tenant.to_string())).await?;

    Ok(rsp)
}

async fn list_tenants(uri: &str) -> Result<Vec<String>, String> {
    let sql = "select * from cluster_schema.tenants".to_string();
    let rsp = cnosdb_http_client(uri, sql, "/api/v1/sql", Some("cnosdb".to_string())).await?;
    if rsp.is_empty() {
        return Ok(vec![]);
    }

    #[derive(Serialize, Deserialize, Debug, Default, Clone)]
    struct Tenant {
        pub tenant_name: String,
        pub tenant_options: String,
    }

    let list = serde_json::from_str::<Vec<Tenant>>(&rsp).map_err(|err| err.to_string())?;

    let mut tenants = vec![];
    for item in list {
        tenants.push(item.tenant_name)
    }

    Ok(tenants)
}

async fn list_databases(uri: &str, tenant: &str) -> Result<Vec<String>, String> {
    let sql = "show databases".to_string();
    let rsp = cnosdb_http_client(uri, sql, "/api/v1/sql", Some(tenant.to_string())).await?;
    if rsp.is_empty() {
        return Ok(vec![]);
    }

    #[derive(Serialize, Deserialize, Debug, Default, Clone)]
    struct Database {
        pub database_name: String,
    }

    let list = serde_json::from_str::<Vec<Database>>(&rsp).map_err(|err| err.to_string())?;

    let mut databases = vec![];
    for item in list {
        databases.push(item.database_name)
    }

    Ok(databases)
}

async fn list_tables(uri: &str, tenant: &str) -> Result<Vec<String>, String> {
    let sql = "show tables".to_string();
    let rsp = cnosdb_http_client(uri, sql, "/api/v1/sql", Some(tenant.to_string())).await?;
    if rsp.is_empty() {
        return Ok(vec![]);
    }

    #[derive(Serialize, Deserialize, Debug, Default, Clone)]
    struct Table {
        pub table_name: String,
    }

    let list = serde_json::from_str::<Vec<Table>>(&rsp).map_err(|err| err.to_string())?;

    let mut tables = vec![];
    for item in list {
        tables.push(item.table_name)
    }

    Ok(tables)
}

// user:pw@127.0.0.1:8888
// user:pw@127.0.0.1:8888/db
async fn cnosdb_http_client(
    uri: &str,
    sql: String,
    path: &str,
    tenant: Option<String>,
) -> Result<String, String> {
    let strs: Vec<&str> = uri.split('@').collect();
    if strs.len() != 2 {
        return Err(format!("uri args not valid: {}", uri));
    }
    let user_pw = strs[0].to_string();

    let strs: Vec<&str> = strs[1].split('/').collect();
    let url = if let Some(tenant) = tenant {
        if strs.len() < 2 {
            format!("http://{}{}?tenant={}", strs[0], path, tenant)
        } else {
            format!(
                "http://{}{}?db={}&tenant={}",
                strs[0], path, strs[1], tenant
            )
        }
    } else {
        format!("http://{}{}", strs[0], path)
    };

    let client = reqwest::Client::new();
    let rsp = client
        .post(&url)
        .header(ACCEPT, "application/json")
        .header(AUTHORIZATION, format!("Basic {}", base64::encode(user_pw)))
        .body(sql)
        .send()
        .await
        .map_err(|err| err.to_string())?
        .text()
        .await
        .map_err(|err| err.to_string())?;

    Ok(rsp)
}

fn data_path() -> String {
    std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned()
        + "/data"
}
