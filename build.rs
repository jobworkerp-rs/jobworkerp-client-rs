use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .file_descriptor_set_path(out_dir.join("jobworkerp_descriptor.bin")) // for reflection
        .compile_protos(
            &[
                // TODO proto file path
                "protobuf/jobworkerp/data/common.proto",
                "protobuf/jobworkerp/data/worker_schema.proto",
                "protobuf/jobworkerp/data/worker.proto",
                "protobuf/jobworkerp/data/job.proto",
                "protobuf/jobworkerp/data/job_result.proto",
                "protobuf/jobworkerp/service/common.proto",
                "protobuf/jobworkerp/service/worker_schema.proto",
                "protobuf/jobworkerp/service/worker.proto",
                "protobuf/jobworkerp/service/job.proto",
                "protobuf/jobworkerp/service/job_result.proto",
            ],
            &["protobuf"],
        )
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
    Ok(())
}
