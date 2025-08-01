use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    tonic_prost_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .file_descriptor_set_path(out_dir.join("jobworkerp_descriptor.bin")) // for reflection
        .type_attribute(
            ".",
            "#[derive(serde::Serialize, serde::Deserialize, schemars::JsonSchema)]",
        )
        .compile_protos(
            &[
                // TODO proto file path
                "protobuf/jobworkerp/data/common.proto",
                "protobuf/jobworkerp/data/runner.proto",
                "protobuf/jobworkerp/data/worker.proto",
                "protobuf/jobworkerp/data/job.proto",
                "protobuf/jobworkerp/data/job_result.proto",
                "protobuf/jobworkerp/service/common.proto",
                "protobuf/jobworkerp/service/runner.proto",
                "protobuf/jobworkerp/service/worker.proto",
                "protobuf/jobworkerp/service/job.proto",
                "protobuf/jobworkerp/service/job_result.proto",
                // functions
                "protobuf/jobworkerp/function/data/function_set.proto",
                "protobuf/jobworkerp/function/data/function.proto",
                "protobuf/jobworkerp/function/service/function_set.proto",
                "protobuf/jobworkerp/function/service/function.proto",
            ],
            &["protobuf"],
        )
        .unwrap_or_else(|e| panic!("Failed to compile protos {e:?}"));
    Ok(())
}
