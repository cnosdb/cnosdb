use std::io;
use std::path::PathBuf;
use std::process::exit;

use config::{meta, tskv};

fn print_usages(exe: &str) {
    eprintln!(
        r#"===== CnosDB Config CLI =====
Generate config files, with environment variables merged.
- To list supported environment variables, run:
  {exe} gen --type data --help
  {exe} gen --type meta --help
- To generate config files, run:
  {exe} gen --type data
  {exe} gen --type meta

Usage: {exe} [COMMAND] [OPTIONS]
Global OPTIONS:
  -v, --version       Show version
  -h, --help          Show this help message
COMMANDs:
  gen [OPTIONS]  Generate config files
    OPTIONS:
      -t, --type <type>   Type of config to generate: data|meta (default: data)
      -o, --output <path> Output file path (default: stdout)
      -h, --help          Show this help message
"#
    );
}

fn main() {
    let mut args = std::env::args();

    let exe = PathBuf::from(
        args.next()
            .expect("std::env::args() should return at least one argument"),
    )
    .file_name()
    .map(|os_str| os_str.to_string_lossy().into_owned())
    .unwrap_or_else(|| "cnosdb-config".to_string());

    match args.next() {
        Some(cmd) => match cmd.as_str() {
            "--version" | "-v" => {
                println!("{exe} {}", version::crate_version!());
            }
            "--help" | "-h" => print_usages(&exe),
            "gen" => {
                let g = GenerateCommand::parse(&mut args);
                g.generate();
            }
            other => {
                eprintln!("Unknown command '{other}'");
                exit(1);
            }
        },
        None => print_usages(&exe),
    }
}

pub struct GenerateCommand {
    /// Type of config to generate.
    type_: GenerateType,
    /// Output file path.
    /// If not specified, output to stdout.
    output: GenerateOutput,

    /// Only to print help message and exit.
    show_help: bool,
}

impl GenerateCommand {
    pub fn parse<I: Iterator<Item = String>>(args: &mut I) -> Self {
        let mut type_ = None::<String>;
        let mut output = None::<String>;
        let mut show_help = false;
        while let Some(arg) = args.next() {
            if arg == "-t" || arg == "--type" {
                type_ = args.next();
            } else if arg == "-o" || arg == "--output" {
                output = args.next();
            } else if arg == "-h" || arg == "--help" {
                show_help = true;
            } else {
                eprintln!("Unknown argument '{arg}'");
                exit(1);
            }
        }
        let type_ = match type_ {
            Some(t) => GenerateType::from(t),
            None => GenerateType::Data,
        };
        let output = match output {
            Some(o) => GenerateOutput::from(o),
            None => GenerateOutput::Stdout,
        };
        Self {
            type_,
            output,
            show_help,
        }
    }

    pub fn generate(&self) {
        let config = match self.type_ {
            GenerateType::Data => {
                if self.show_help {
                    println!("All supported environment variables:");
                    tskv::Config::env_keys().into_keys().for_each(|k| {
                        println!("{k}");
                    });
                    exit(0);
                }

                let cfg = tskv::Config::new(None::<&str>).unwrap_or_else(|e| {
                    eprintln!("Failed to load config: {e}.");
                    exit(1);
                });
                cfg.to_string_pretty()
            }
            GenerateType::Meta => {
                if self.show_help {
                    println!("All supported environment variables:");
                    meta::Opt::env_keys().into_keys().for_each(|k| {
                        println!("{k}");
                    });
                    exit(0);
                }

                let cfg = meta::Opt::new(None::<&str>).unwrap_or_else(|e| {
                    eprintln!("Failed to load config: {e}.");
                    exit(1);
                });
                cfg.to_string_pretty()
            }
        };

        let mut writer: Box<dyn io::Write> = match &self.output {
            GenerateOutput::Stdout => Box::new(io::stdout()),
            GenerateOutput::File(path) => {
                let file = std::fs::File::create(path).unwrap_or_else(|e| {
                    eprintln!("Failed to create config file '{path}': {e}.");
                    exit(1);
                });
                eprintln!("Writing config file: '{path}'.");
                Box::new(file)
            }
        };

        writer.write_all(config.as_bytes()).unwrap_or_else(|e| {
            eprintln!("Failed to write to output: {e}");
            exit(1);
        });
    }
}

pub enum GenerateType {
    /// Generate data node config.
    Data,
    /// Generate meta node config.
    Meta,
}

impl From<String> for GenerateType {
    fn from(s: String) -> Self {
        match s.as_str() {
            "data" => GenerateType::Data,
            "meta" => GenerateType::Meta,
            _ => {
                eprintln!("Invalid -t|--type '{s}'");
                exit(1);
            }
        }
    }
}

pub enum GenerateOutput {
    /// Write to stdout.
    Stdout,
    /// Write to file (path).
    File(String),
}

impl From<String> for GenerateOutput {
    fn from(s: String) -> Self {
        match s.as_str() {
            "stdout" => GenerateOutput::Stdout,
            _ => GenerateOutput::File(s),
        }
    }
}
