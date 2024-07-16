use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::{cmp, env, fs};

type AnyError = Box<dyn std::error::Error>;

const VERSION_PROTOBUF: Version = Version(27, 0, 0); // 27.0
const VERSION_FLATBUFFERS: Version = Version(24, 3, 25); // 24.3.25

/// The folder where the build script should place its output.
/// https://doc.rust-lang.org/cargo/reference/environment-variables.html
const ENV_OUT_DIR: &str = "OUT_DIR";

/// Path of `flatc` binary.
const ENV_FLATC_PATH: &str = "FLATC_PATH";

/// Build protos if the major version of `flatc` or `protoc` is greater
/// or lesser than the expected version.
const ENV_BUILD_PROTOS: &str = "BUILD_PROTOS";

fn main() -> Result<(), AnyError> {
    println!("cargo:rerun-if-changed=proto");
    println!("cargo:rerun-if-changed=prompb");

    let project_root_dir = env::current_dir()?;
    let proto_dir = project_root_dir.join("proto");
    let prompb_dir = project_root_dir.join("prompb");
    let rust_dir = env::current_dir().unwrap().join("src");

    // src/generated/mod.rs
    let generated_mod_rs_path = project_root_dir
        .join("src")
        .join("generated")
        .join("mod.rs");
    let mut generated_mod_rs = fs::File::create(generated_mod_rs_path)?;
    writeln!(&mut generated_mod_rs, "#![allow(unused_imports)]")?;
    writeln!(&mut generated_mod_rs, "#![allow(clippy::all)]")?;
    generated_mod_rs.flush()?;

    // build proto/*.proto files to src/generated/protobuf_generated/
    compile_protobuf_models(
        &mut generated_mod_rs,
        &proto_dir,
        &rust_dir.join("generated").join("protobuf_generated"),
        vec![
            ("kv_service.proto", "kv_service"),
            ("vector_event.proto", "vector"),
            ("raft_service.proto", "raft_service"),
            ("logproto.proto", "logproto"),
            ("jaeger_api_v2.proto", "jaeger_api_v2"),
            ("jaeger_storage_v1.proto", "jaeger_storage_v1"),
            ("otlp_common.proto", "common"),
            ("otlp_resource.proto", "resource"),
            ("otlp_trace.proto", "trace"),
            ("otlp_trace_service.proto", "trace_service"),
        ],
    )?;

    let flatc_path = match env::var(ENV_FLATC_PATH) {
        Ok(path) => {
            println!("cargo:warning=Specified flatc path by environment {ENV_FLATC_PATH}={path}");
            path
        }
        Err(_) => "flatc".to_string(),
    };

    // build proto/*.fbs files to src/generated/flatbuffers_generated/
    compile_flatbuffers_models(
        &mut generated_mod_rs,
        &flatc_path,
        &proto_dir,
        &rust_dir.join("generated").join("flatbuffers_generated"),
        vec!["models"],
    )?;

    // build prompb/*.proto files to src/prompb/
    compile_prometheus_models(&prompb_dir, &rust_dir.join("prompb"))?;

    Ok(())
}

/// Compile *.proto files
fn compile_protobuf_models<P: AsRef<Path>, S: AsRef<str>>(
    generated_mod_rs: &mut fs::File,
    in_proto_dir: P,
    out_rust_dir: P,
    proto_package_names: Vec<(S, S)>,
) -> Result<(), AnyError> {
    let version = protobuf_compiler_version()?;
    let need_compile = match version.compare_ext(&VERSION_PROTOBUF) {
        Ok(cmp::Ordering::Equal) => true,
        Ok(_) => {
            let version_err = Version::build_error_message(&version, &VERSION_PROTOBUF).unwrap();
            println!("cargo:warning=Tool `protoc` {version_err}, skip compiling.");
            false
        }
        Err(version_err) => {
            // return Err(format!("Tool `protoc` {version_err}, please update it.").into());
            println!("cargo:warning=Tool `protoc` {version_err}, please update it.");
            false
        }
    };

    let proto_dir = in_proto_dir.as_ref();
    let rust_dir = out_rust_dir.as_ref();
    fs::create_dir_all(rust_dir)?;

    let proto_file_paths = proto_package_names
        .iter()
        .map(|(proto, _package)| proto.as_ref().to_string())
        .collect::<Vec<String>>();

    let descriptor_set_path =
        PathBuf::from(env::var(ENV_OUT_DIR).unwrap()).join("proto-descriptor.bin");

    if need_compile {
        tonic_build::configure()
            .out_dir(rust_dir)
            .file_descriptor_set_path(descriptor_set_path)
            .protoc_arg("--experimental_allow_proto3_optional")
            .compile_well_known_types(true)
            .emit_rerun_if_changed(false)
            .compile(&proto_file_paths, &[proto_dir])
            .map_err(|e| format!("Failed to generate protobuf file: {e}."))?;
    }

    // $rust_dir/mod.rs
    let mut sub_mod_rs = fs::File::create(rust_dir.join("mod.rs"))?;
    for (_proto, package) in proto_package_names.iter() {
        let mod_name = package.as_ref();
        if mod_name == "jaeger_storage_v1" {
            writeln!(&mut sub_mod_rs, "#[path = \"jaeger.storage.v1.rs\"]")?;
        } else if mod_name == "common" {
            writeln!(
                &mut sub_mod_rs,
                "#[path = \"opentelemetry.proto.common.rs\"]"
            )?;
        } else if mod_name == "resource" {
            writeln!(
                &mut sub_mod_rs,
                "#[path = \"opentelemetry.proto.resource.rs\"]"
            )?;
        } else if mod_name == "trace" {
            writeln!(
                &mut sub_mod_rs,
                "#[path = \"opentelemetry.proto.trace.rs\"]"
            )?;
        } else if mod_name == "trace_service" {
            writeln!(
                &mut sub_mod_rs,
                "#[path = \"opentelemetry.proto.collector.trace.rs\"]"
            )?;
        }
        writeln!(&mut sub_mod_rs, "pub mod {mod_name};")?;
    }
    sub_mod_rs.flush()?;

    writeln!(generated_mod_rs)?;
    writeln!(generated_mod_rs, "mod protobuf_generated;")?;
    writeln!(generated_mod_rs, "pub use protobuf_generated::*;")?;
    generated_mod_rs.flush()?;

    Ok(())
}

/// Compile proto/**.fbs files.
fn compile_flatbuffers_models<P: AsRef<Path>, S: AsRef<str>>(
    generated_mod_rs: &mut fs::File,
    flatc_path: &str,
    in_fbs_dir: P,
    out_rust_dir: P,
    mod_names: Vec<S>,
) -> Result<(), AnyError> {
    let version = flatbuffers_compiler_version(flatc_path)?;
    let need_compile = match version.compare_ext(&VERSION_FLATBUFFERS) {
        Ok(cmp::Ordering::Equal) => true,
        Ok(_) => {
            let version_err = Version::build_error_message(&version, &VERSION_FLATBUFFERS).unwrap();
            println!("cargo:warning=Tool `{flatc_path}` {version_err}, skip compiling.");
            false
        }
        Err(version_err) => {
            return Err(format!("Tool `{flatc_path}` {version_err}, please update it.").into());
        }
    };

    let fbs_dir = in_fbs_dir.as_ref();
    let rust_dir = out_rust_dir.as_ref();
    fs::create_dir_all(rust_dir)?;

    // $rust_dir/mod.rs
    let mut sub_mod_rs = fs::File::create(rust_dir.join("mod.rs"))?;
    writeln!(generated_mod_rs)?;
    writeln!(generated_mod_rs, "mod flatbuffers_generated;")?;
    for mod_name in mod_names.iter() {
        let mod_name = mod_name.as_ref();
        writeln!(
            generated_mod_rs,
            "pub use flatbuffers_generated::{mod_name}::*;"
        )?;
        writeln!(&mut sub_mod_rs, "pub mod {mod_name};")?;

        if need_compile {
            let fbs_file_path = fbs_dir.join(format!("{mod_name}.fbs"));
            let output = Command::new(flatc_path)
                .arg("-o")
                .arg(rust_dir)
                .arg("--rust")
                .arg("--gen-mutable")
                .arg("--gen-onefile")
                .arg("--gen-name-strings")
                .arg("--filename-suffix")
                .arg("")
                .arg(&fbs_file_path)
                .output()
                .map_err(|e| format!("Failed to execute process of flatc: {e}"))?;
            if !output.status.success() {
                return Err(format!(
                    "Failed to generate file '{}' by flatc(path: '{flatc_path}'): {}.",
                    fbs_file_path.display(),
                    String::from_utf8_lossy(&output.stderr),
                )
                .into());
            }
        }
    }
    generated_mod_rs.flush()?;
    sub_mod_rs.flush()?;

    Ok(())
}

/// Compile types.proto and remote.proto.
fn compile_prometheus_models<P: AsRef<Path>>(
    in_proto_dir: P,
    out_rust_dir: P,
) -> Result<(), AnyError> {
    let proto_dir = in_proto_dir.as_ref();
    let rust_dir = out_rust_dir.as_ref();
    fs::create_dir_all(rust_dir)?;

    let descriptor_set_path =
        PathBuf::from(env::var(ENV_OUT_DIR).unwrap()).join("proto-descriptor.bin");

    tonic_build::configure()
        .out_dir(rust_dir)
        .file_descriptor_set_path(descriptor_set_path)
        .compile_well_known_types(true)
        .emit_rerun_if_changed(false)
        .compile(&["types.proto", "remote.proto"], &[proto_dir])
        .map_err(|e| format!("Failed to generate protobuf file: {e}."))?;

    // $rust_dir/mod.rs
    let mut sub_mod_rs = fs::File::create(rust_dir.join("mod.rs"))?;
    writeln!(&mut sub_mod_rs, "pub mod prometheus;")?;
    sub_mod_rs.flush()?;

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Version(u32, u32, u32);

impl Version {
    fn try_get<F: FnOnce(&str) -> Result<String, String>>(
        exe: String,
        output_to_version_string: F,
    ) -> Result<Self, String> {
        let cmd = format!("{exe} --version");
        let output = std::process::Command::new(exe)
            .arg("--version")
            .output()
            .map_err(|e| format!("Failed to execute `{cmd}`: {e}",))?;
        let output_utf8 = String::from_utf8(output.stdout).map_err(|e| {
            let output_lossy = String::from_utf8_lossy(e.as_bytes());
            format!("Command `{cmd}` returned invalid UTF-8('{output_lossy}'): {e}")
        })?;
        if output.status.success() {
            let version_string = output_to_version_string(&output_utf8)?;
            Ok(version_string.parse::<Self>()?)
        } else {
            Err(format!(
                "Failed to get version by command `{cmd}`: {output_utf8}"
            ))
        }
    }

    fn build_error_message(version: &Self, expected: &Self) -> Option<String> {
        match version.compare_major_version(expected) {
            cmp::Ordering::Equal => None,
            cmp::Ordering::Greater => Some(format!(
                "version({version}) is greater than version({expected})"
            )),
            cmp::Ordering::Less => Some(format!(
                "version({version}) is lesser than version({expected})"
            )),
        }
    }

    fn compare_ext(&self, expected_version: &Self) -> Result<cmp::Ordering, String> {
        match env::var(ENV_BUILD_PROTOS) {
            Ok(build_protos) => {
                if build_protos.is_empty() || build_protos == "0" {
                    Ok(self.compare_major_version(expected_version))
                } else {
                    match self.compare_major_version(expected_version) {
                        cmp::Ordering::Equal => Ok(cmp::Ordering::Equal),
                        _ => Err(Self::build_error_message(self, expected_version).unwrap()),
                    }
                }
            }
            Err(_) => Ok(self.compare_major_version(expected_version)),
        }
    }

    fn compare_major_version(&self, other: &Self) -> cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl std::str::FromStr for Version {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut version = [0_u32; 3];
        for (i, v) in s.split('.').take(3).enumerate() {
            version[i] = v
                .parse()
                .map_err(|e| format!("Failed to parse version string '{s}': {e}"))?;
        }
        Ok(Version(version[0], version[1], version[2]))
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.0, self.1, self.2)
    }
}

/// Run command `flatc --version` to get the version of flatc.
///
/// ```ignore
/// $ flatc --version
/// flatc version 24.3.25
/// ```
fn flatbuffers_compiler_version(flatc_path: impl AsRef<Path>) -> Result<Version, String> {
    let flatc_path = flatc_path.as_ref();
    Version::try_get(format!("{}", flatc_path.display()), |output| {
        const PREFIX_OF_VERSION: &str = "flatc version ";
        let output = output.trim();
        if let Some(version) = output.strip_prefix(PREFIX_OF_VERSION) {
            Ok(version.to_string())
        } else {
            Err(format!("Failed to get flatc version: {output}"))
        }
    })
}

/// Run command `protoc --version` to get the version of flatc.
///
/// ```ignore
/// $ protoc --version
/// libprotoc 27.0
/// ```
fn protobuf_compiler_version() -> Result<Version, String> {
    Version::try_get("protoc".to_string(), |output| {
        const PREFIX_OF_VERSION: &str = "libprotoc ";
        let output = output.trim();
        if let Some(version) = output.strip_prefix(PREFIX_OF_VERSION) {
            Ok(version.to_string())
        } else {
            Err(format!("Failed to get protoc version: {output}"))
        }
    })
}
