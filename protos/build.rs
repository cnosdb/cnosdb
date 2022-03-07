use prost_build::Config;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::{env, fs};

fn build_protobuf(
    protos: &[impl AsRef<Path>],
    includes: &[impl AsRef<Path>],
    config: Config,
    descriptor_set_path: &impl AsRef<Path>,
    out_dir: &impl AsRef<Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .out_dir(out_dir)
        .file_descriptor_set_path(descriptor_set_path)
        .compile_well_known_types(true)
        .format(true)
        .compile_with_config(config, protos, includes)?;

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let project_root_dir = env::current_dir()?;
    let proto_files_dir = project_root_dir.join("proto");

    // build .proto files
    {
        let proto_file_paths = &[proto_files_dir.join("tskv.proto")];

        let output_dir_final = PathBuf::from(env::var("OUT_DIR").unwrap()).join("protobuf");
        fs::create_dir_all(&output_dir_final)?;
        let config = prost_build::Config::new();
        let descriptor_set_path =
            PathBuf::from(env::var("OUT_DIR").unwrap()).join("proto-descriptor.bin");
        build_protobuf(
            proto_file_paths,
            &[proto_files_dir.as_path()],
            config,
            &descriptor_set_path,
            &output_dir_final,
        )?;
    }

    // build .fbs files
    {
        let fbs_file_paths = &[proto_files_dir.join("models.fbs")];
        let output_dir_final = PathBuf::from(env::var("OUT_DIR").unwrap()).join("flatbuffers");
        fs::create_dir_all(&output_dir_final)?;

        for p in fbs_file_paths.into_iter() {
            let mut output_file_name = p
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
            output_file_name.push_str(".rs");

            Command::new("flatc")
                .arg("-o")
                .arg(&output_dir_final)
                .arg("--rust")
                .arg("--gen-onefile")
                .arg("--gen-name-strings")
                .arg("--filename-suffix")
                .arg("")
                .arg(p)
                .output()
                .expect(&*format!("Failed to generate file {}.", output_file_name));

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
