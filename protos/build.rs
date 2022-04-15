use core::panic;
use prost_build;
use std::io::Write;
use std::ops::Deref;
use std::path::PathBuf;
use std::process::Command;
use std::{env, fs};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let project_root_dir = env::current_dir()?;
    let proto_files_dir = project_root_dir.join("proto");

    // src/generated/mod.rs
    let generated_mod_rs_path = project_root_dir
        .join("src")
        .join("generated")
        .join("mod.rs");
    let mut generated_mod_rs_file = fs::File::create(&generated_mod_rs_path)?;
    generated_mod_rs_file.write_all(
        b"mod protobuf_generated;
pub use protobuf_generated::*;

mod flatbuffers_generated;
",
    )?;

    // build .proto files
    {
        let proto_file_paths = &[proto_files_dir.join("kv_service.proto")];
        let rust_mod_names = &["kv_service".to_string()];

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
            .file_descriptor_set_path(&descriptor_set_path)
            .compile_well_known_types(true)
            .format(true)
            .compile_with_config(config, proto_file_paths, &[proto_files_dir.as_path()])
            .expect("Failed to generate protobuf file {}.");

        // src/generated/protobuf_generated/mod.rs
        let mut protobuf_generated_mod_rs_file = fs::File::create(output_dir_final.join("mod.rs"))?;
        for mod_name in rust_mod_names.into_iter() {
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
        for p in fbs_file_paths.into_iter() {
            let output_rust_mod_name = p
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .split('.')
                .collect::<Vec<&str>>()
                .get(0)
                .unwrap()
                .deref()
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

            let output = Command::new("flatc")
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
                .expect(&*format!(
                    "Failed to generate file by flatbuffers {}.",
                    output_file_name
                ));

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
