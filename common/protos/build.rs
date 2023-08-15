use core::panic;
use std::{env, fs, io::Write, path::PathBuf, process::Command};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let project_root_dir = env::current_dir()?;
    let proto_files_dir = project_root_dir.join("proto");

    // src/generated/mod.rs
    let generated_mod_rs_path = project_root_dir
        .join("src")
        .join("generated")
        .join("mod.rs");
    let mut generated_mod_rs_file = fs::File::create(generated_mod_rs_path)?;
    generated_mod_rs_file.write_all(
        b"#![allow(unused_imports)]
#![allow(clippy::all)]
mod protobuf_generated;
pub use protobuf_generated::*;

mod flatbuffers_generated;
",
    )?;

    // build .proto files
    {
        let proto_file_paths = &[
            proto_files_dir.join("kv_service.proto"),
            proto_files_dir.join("vector_event.proto"),
            proto_files_dir.join("raft_service.proto"),
        ];
        let rust_mod_names = &[
            "kv_service".to_string(),
            "vector".to_string(),
            "raft_service".to_string(),
        ];

        // src/generated/protobuf_generated/
        let output_dir_final = env::current_dir()
            .unwrap()
            .join("src")
            .join("generated")
            .join("protobuf_generated");
        fs::create_dir_all(&output_dir_final)?;
        let config = prost_build::Config::new();
        let descriptor_set_path =
            PathBuf::from(env::var("OUT_DIR").unwrap()).join("proto-descriptor.bin");

        tonic_build::configure()
            .out_dir(&output_dir_final)
            .file_descriptor_set_path(descriptor_set_path)
            .protoc_arg("--experimental_allow_proto3_optional")
            .compile_well_known_types(true)
            .compile_with_config(config, proto_file_paths, &[proto_files_dir.as_path()])
            .expect("Failed to generate protobuf file {}.");

        // src/generated/protobuf_generated/mod.rs
        let mut protobuf_generated_mod_rs_file = fs::File::create(output_dir_final.join("mod.rs"))?;
        for mod_name in rust_mod_names.iter() {
            protobuf_generated_mod_rs_file.write_all(b"pub mod ")?;
            protobuf_generated_mod_rs_file.write_all(mod_name.as_bytes())?;
            protobuf_generated_mod_rs_file.write_all(b";\n")?;
            protobuf_generated_mod_rs_file.flush()?;
        }
    }

    // build .fbs files
    {
        let fbs_file_paths = &[proto_files_dir.join("models.fbs")];

        // src/generated/flatbuffers_generated/
        let output_dir_final = env::current_dir()
            .unwrap()
            .join("src")
            .join("generated")
            .join("flatbuffers_generated");
        fs::create_dir_all(&output_dir_final)?;

        // src/generated/flatbuffers_generated/mod.rs
        let mut flatbuffers_generated_mod_rs_file =
            fs::File::create(output_dir_final.join("mod.rs"))?;

        // <flatbuffers_file_name>.fbs -> <flatbuffers_file_name>
        for p in fbs_file_paths.iter() {
            let output_rust_mod_name = p
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .split('.')
                .collect::<Vec<&str>>()
                .first()
                .unwrap()
                .to_string();

            // <flatbuffers_file_name> -> <flatbuffers_file_name>.rs
            let output_file_name = output_rust_mod_name.clone() + ".rs";

            generated_mod_rs_file.write_all(b"pub use flatbuffers_generated::")?;
            generated_mod_rs_file.write_all(output_rust_mod_name.as_bytes())?;
            generated_mod_rs_file.write_all(b"::*;\n")?;
            generated_mod_rs_file.flush()?;

            flatbuffers_generated_mod_rs_file.write_all(b"pub mod ")?;
            flatbuffers_generated_mod_rs_file.write_all(output_rust_mod_name.as_bytes())?;
            flatbuffers_generated_mod_rs_file.write_all(b";\n")?;
            flatbuffers_generated_mod_rs_file.flush()?;

            let flatc_path = match env::var("FLATC_PATH") {
                Ok(p) => {
                    println!(
                        "Found specified flatc path in environment FLATC_PATH( {} )",
                        &p
                    );
                    p
                }
                Err(_) => "flatc".to_string(),
            };
            let output = Command::new(&flatc_path)
                .arg("-o")
                .arg(&output_dir_final)
                .arg("--rust")
                .arg("--gen-mutable")
                .arg("--gen-onefile")
                .arg("--gen-name-strings")
                .arg("--filename-suffix")
                .arg("")
                .arg(p)
                .output()
                .unwrap_or_else(|e| {
                    panic!(
                        "Failed to generate file '{}' by flatc(path: '{}'): {:?}.",
                        output_file_name, flatc_path, e
                    )
                });

            if !output.status.success() {
                panic!("{}", String::from_utf8(output.stderr).unwrap());
            }

            let output_file_path = output_dir_final.join(output_file_name);
            println!("fbs_file: {}", output_file_path.to_str().unwrap());

            Command::new("rustfmt")
                .arg(output_file_path)
                .output()
                .expect("Failed to format file.");
        }
    }

    Ok(())
}
