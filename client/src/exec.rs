//! Execution functions

use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::time::Instant;

use rustyline::error::ReadlineError;
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
) {
    let mut query = "".to_owned();

    for line in reader.lines() {
        match line {
            Ok(line) if line.starts_with("--") => {
                continue;
            }
            Ok(line) => {
                let line = line.trim_end();
                query.push_str(line);
                if line.ends_with(';') {
                    match exec_and_print(ctx, print_options, query).await {
                        Ok(_) => {}
                        Err(err) => println!("{:?}", err),
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

    // run the left over query if the last statement doesn't contain ‘;’
    if !query.is_empty() {
        match exec_and_print(ctx, print_options, query).await {
            Ok(_) => {}
            Err(err) => println!("{:?}", err),
        }
    }
}

pub async fn exec_from_files(
    files: Vec<String>,
    ctx: &mut SessionContext,
    print_options: &PrintOptions,
) {
    let files = files
        .into_iter()
        .map(|file_path| File::open(file_path).unwrap())
        .collect::<Vec<_>>();
    for file in files {
        let mut reader = BufReader::new(file);
        exec_from_lines(ctx, &mut reader, print_options).await;
    }
}

/// run and execute SQL statements and commands against a context with the given print options
pub async fn exec_from_repl(ctx: &mut SessionContext, print_options: &mut PrintOptions) {
    let mut rl = Editor::<CliHelper>::new();
    rl.set_helper(Some(CliHelper::default()));
    rl.load_history(".history").ok();

    let mut print_options = print_options.clone();

    loop {
        match rl.readline(format!("{} ❯ ", ctx.get_database()).as_str()) {
            Ok(line) if line.starts_with('\\') => {
                rl.add_history_entry(line.trim_end());
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

            Ok(line) if use_database(&line).is_some() => {
                if let Some(db) = use_database(&line) {
                    if connect_database(&db, ctx).await.is_err() {
                        println!("Cannot connect to database {}.", db);
                    }
                }
            }

            Ok(line) => {
                rl.add_history_entry(line.trim_end());
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
    ctx: &mut SessionContext,
    print_options: &PrintOptions,
    sql: String,
) -> Result<()> {
    let strs: Vec<&str> = sql.split(';').collect();
    for tmp in strs.iter() {
        if tmp.trim().is_empty() {
            continue;
        }

        let now = Instant::now();
        let results = ctx.sql(tmp.to_string() + ";").await?;
        print_options.print_batches(&results, now)?;
    }

    Ok(())
}

fn use_database(sql: &str) -> Option<String> {
    let sql = sql.trim().trim_end_matches(';');
    if !sql[0..3].to_ascii_lowercase().eq("use") {
        return None;
    }

    let sql = sql[3..].trim();

    if sql.starts_with('"') && sql.ends_with('"') {
        Some(sql[1..sql.len() - 1].to_string())
    } else {
        Some(sql.to_string())
    }
}

pub fn is_system_table_db(db: &str) -> bool {
    let db = db.to_ascii_lowercase();
    db.eq("cluster_schema") || db.eq("information_schema")
}

pub async fn connect_database(database: &str, ctx: &mut SessionContext) -> Result<()> {
    if is_system_table_db(database) {
        ctx.set_database(database);
        return Ok(());
    }
    let old_database = ctx.get_database().to_string();
    ctx.set_database(database);
    ctx.sql(format!("DESCRIBE DATABASE {}", database))
        .await
        .map_err(|e| {
            ctx.set_database(old_database.as_str());
            e
        })
        .map(|_| ())
}
