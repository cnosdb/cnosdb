//! Execution functions

use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::time::Instant;

use anyhow::bail;
use futures_util::TryStreamExt;
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use rustyline::Editor;

use crate::command::{Command, OutputFormat};
use crate::ctx::SessionContext;
use crate::helper::CliHelper;
use crate::print_options::PrintOptions;
use crate::Result;

/// run and execute SQL statements and commands from a file, against a context with the given print options
pub async fn exec_from_lines(
    ctx: &mut SessionContext,
    reader: &mut BufReader<File>,
    print_options: &PrintOptions,
) -> Result<()> {
    let mut query = "".to_owned();

    for line in reader.lines() {
        match line {
            Ok(line) if line.starts_with("--") => {
                continue;
            }
            Ok(line) if line.trim().starts_with("\\change_tenant") => {
                let tenant = line.trim().trim_start_matches("\\change_tenant").trim();
                if ctx.get_session_config().process_cli_command {
                    ctx.set_tenant(tenant.to_string());
                } else {
                    bail!("Can't process \"\\change_tenant {}\", please add arg --process_cli_command", tenant)
                }
            }
            Ok(line) if line.starts_with("\\c") => {
                let database = line.trim_start_matches("\\c").trim();
                if ctx.get_session_config().process_cli_command {
                    ctx.set_database(database);
                } else {
                    bail!(
                        "Can't process \"\\c {}\", please add arg --process_cli_command",
                        database
                    )
                }
            }
            Ok(line) => {
                let line = line.trim_end();
                query.push_str(line);
                if line.ends_with(';') {
                    match exec_and_print(ctx, print_options, query.clone()).await {
                        Ok(_) => {}
                        Err(err) => {
                            eprintln!("{:?}", err);
                            if ctx.get_session_config().error_stop {
                                bail!("{} execute fail, STOP!", query)
                            }
                        }
                    }
                    query = "".to_owned();
                } else {
                    query.push('\n');
                }
            }
            _ => {
                break;
            }
        }
    }

    // run the left over query if the last statement doesn't contain ';'
    if !query.is_empty() {
        match exec_and_print(ctx, print_options, query.clone()).await {
            Ok(_) => {}
            Err(err) => {
                eprintln!("{:?}", err);
                if ctx.get_session_config().error_stop {
                    bail!("{} execute fail, STOP!", query)
                }
            }
        }
    }
    Ok(())
}

pub async fn exec_from_files(
    files: Vec<String>,
    ctx: &mut SessionContext,
    print_options: &PrintOptions,
) -> Result<()> {
    let files = files
        .into_iter()
        .map(|file_path| File::open(&file_path).map(|f| (file_path, f)))
        .collect::<Vec<_>>();
    for file in files {
        let (path, file) = file?;
        let mut reader = BufReader::new(file);
        if let Err(e) = exec_from_lines(ctx, &mut reader, print_options).await {
            bail!("Execute file {} fail, Error: {}", path, e)
        };
    }
    Ok(())
}

/// run and execute SQL statements and commands against a context with the given print options
pub async fn exec_from_repl(ctx: &mut SessionContext, print_options: &PrintOptions) {
    let mut rl = Editor::<CliHelper, DefaultHistory>::new().unwrap();
    rl.set_helper(Some(CliHelper::default()));
    rl.load_history(".history").ok();

    let mut print_options = print_options.clone();

    loop {
        match rl.readline(format!("{} ❯ ", ctx.get_database()).as_str()) {
            Ok(line) if line.trim().starts_with('\\') => {
                rl.add_history_entry(line.trim_end()).unwrap();
                let command = line.split_whitespace().collect::<Vec<_>>().join(" ");
                if let Ok(cmd) = &command[1..].parse::<Command>() {
                    match cmd {
                        Command::Quit => break,
                        Command::OutputFormat(subcommand) => {
                            if let Some(subcommand) = subcommand {
                                if let Ok(command) = subcommand.parse::<OutputFormat>() {
                                    if let Err(e) = command.execute(&mut print_options).await {
                                        eprintln!("{}", e)
                                    }
                                } else {
                                    eprintln!("'\\{}' is not a valid command", &line[1..]);
                                }
                            } else {
                                println!("Output format is {:?}.", print_options.format);
                            }
                        }
                        _ => {
                            if let Err(e) = cmd.execute(ctx, &mut print_options).await {
                                eprintln!("{e}")
                            }
                        }
                    }
                } else {
                    eprintln!("'\\{}' is not a valid command", &line[1..]);
                }
            }

            Ok(line) if parse_use_database(&line).is_some() => {
                if let Some(db) = parse_use_database(&line) {
                    if connect_database(&db, ctx).await.is_err() {
                        eprintln!("Cannot use database {}.", db);
                    }
                }
            }

            Ok(line) => {
                rl.add_history_entry(line.trim_end()).unwrap();
                match exec_and_print(ctx, &print_options, line).await {
                    Ok(_) => {}
                    Err(err) => eprintln!("{:?}", err),
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!("\\q");
                break;
            }
            Err(err) => {
                eprintln!("Unknown error happened {:?}", err);
                break;
            }
        }
    }

    rl.save_history(".history").ok();
}

async fn exec_and_print(
    ctx: &SessionContext,
    print_options: &PrintOptions,
    sql: String,
) -> Result<()> {
    let (mut single_quote, mut double_quote) = (false, false);
    let (mut last_idx, mut sql_idx) = (0, 0);

    let mut strs: Vec<&str> = Vec::new();
    for ch in sql.chars() {
        match ch {
            '\'' => {
                if !double_quote {
                    single_quote = !single_quote;
                }
            }
            '\"' => {
                if !single_quote {
                    double_quote = !double_quote;
                }
            }
            ';' => {
                if !double_quote && !single_quote {
                    strs.push(&sql[last_idx..sql_idx]);
                    last_idx = sql_idx + 1;
                }
            }
            _ => (),
        };
        sql_idx += ch.len_utf8();
    }

    if strs.is_empty() {
        strs.push(&sql);
    }

    for tmp in strs.iter() {
        if tmp.trim().is_empty() {
            continue;
        }

        let now = Instant::now();
        let resp = ctx.sql(tmp.to_string() + ";").await?;
        if ctx.get_session_config().chunked {
            let header = resp.headers().clone();
            let mut stream = resp.bytes_stream();
            loop {
                let chunk = stream
                    .try_next()
                    .await
                    .map_err(|e| anyhow::anyhow!(e.to_string()))?;
                if let Some(chunk) = chunk {
                    let results = SessionContext::decode_body(chunk, &header).await?;
                    print_options.print_stream_batches(Some(&results), now)?;
                } else {
                    print_options.print_stream_batches(None, now)?;
                    break;
                }
            }
        } else {
            let results = SessionContext::parse_response(resp).await?;
            print_options.print_batches(&results, now)?;
        }
    }

    Ok(())
}

fn parse_use_database(sql: &str) -> Option<String> {
    let sql = sql.trim().trim_end_matches(';');
    if !sql.to_ascii_lowercase().starts_with("use") {
        return None;
    }

    let database = sql[3..].trim();

    if database.starts_with('"') && database.ends_with('"') {
        Some(database.trim_matches('"').to_string())
    } else if database.starts_with('\'') && database.ends_with('\'') {
        Some(database.trim_matches('\'').to_string())
    } else {
        if database.contains(' ') {
            return None;
        }
        // normalize ident
        Some(database.to_ascii_lowercase())
    }
}

pub fn is_system_table_db(db: &str) -> bool {
    let db = db.to_ascii_lowercase();
    db.eq("cluster_schema") || db.eq("information_schema") || db.eq("usage_schema")
}

pub async fn connect_database(database: &str, ctx: &mut SessionContext) -> Result<()> {
    if is_system_table_db(database) {
        ctx.set_database(database);
        return Ok(());
    }
    let old_database = ctx.get_database().to_string();
    ctx.set_database(database);
    ctx.sql(format!("DESCRIBE DATABASE \"{}\"", database))
        .await
        .map_err(|e| {
            ctx.set_database(old_database.as_str());
            e
        })
        .map(|_| ())
}
