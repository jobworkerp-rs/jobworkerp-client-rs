// command line function to interact with jobworkerp grpc api (CRUD)
// arguments:
//
// command: jobworkerp-cli
//
// subcommands:
// - runner
// - worker
// - job
// - job_result
//
// subcommands arguments:
// - runner: find, delete, count
// - worker: create, find, update, delete, count
// - job: create, find, delete, count
// - job_result: find, delete, count
//
// options:
// common:
// -a, --address <address> grpc server address
// -t, --timeout <timeout> request timeout
// --offset <offset> offset of the list (for list)
// --limit <limit> limit of the list (for list)
//
//
//
// as job_result: valid commands are find, list, delete
// -i, --id <id> id of the job

use clap::Parser;
use command_utils::util::tracing::LoggingConfig;
use jobworkerp_client::{
    client::JobworkerpClient,
    command::{
        function::FunctionArg, function_set::FunctionSetArg, job::JobArg, job_result::JobResultArg,
        job_status::JobStatusArg, runner::RunnerArg, worker::WorkerArg,
        worker_instance::WorkerInstanceArg,
    },
};
use std::{collections::HashMap, sync::Arc, time::Duration};

#[derive(Parser, Debug)]
#[clap(name = "jobworkerp-cli", version = "0.5.2", author = "sutr")]
struct Opts {
    #[clap(short, long, default_value = "http://localhost:9000")]
    address: String,
    #[clap(short, long)]
    timeout: Option<u64>,
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Parser, Debug)]
pub(crate) enum SubCommand {
    Runner(RunnerArg),
    Worker(WorkerArg),
    Function(FunctionArg),
    FunctionSet(FunctionSetArg),
    Job(JobArg),
    JobResult(JobResultArg),
    JobStatus(JobStatusArg),
    WorkerInstance(WorkerInstanceArg),
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    // node specific str (based on ip)
    let log_filename =
        command_utils::util::tracing::create_filename_with_ip_postfix("jobworkerp-client", "log");
    let conf = command_utils::util::tracing::load_tracing_config_from_env().unwrap_or_default();
    command_utils::util::tracing::tracing_init(LoggingConfig {
        file_name: Some(log_filename),
        ..conf
    })
    .await
    .unwrap();
    let session_id = std::env::var("SESSION_ID").unwrap_or_else(|_| {
        tracing::info!("SESSION_ID not set, generating a new one");
        uuid::Uuid::new_v4().to_string()
    });
    let user_id = std::env::var("USER_ID").unwrap_or_else(|_| "unknown".to_string());
    let metadata = HashMap::from([
        ("session_id".to_string(), session_id),
        ("user_id".to_string(), user_id),
    ]);

    let opts: Opts = Opts::parse();
    let address = opts.address.clone();
    let timeout = opts.timeout.map(Duration::from_millis);
    let client = JobworkerpClient::new(address, timeout).await.unwrap();
    match opts.subcmd {
        SubCommand::Runner(cmd) => {
            cmd.cmd.execute(&client, &metadata).await;
        }
        SubCommand::Worker(cmd) => {
            cmd.cmd.execute(&client, &metadata).await;
        }
        SubCommand::Function(cmd) => {
            cmd.cmd.execute(&client, &metadata).await;
        }
        SubCommand::FunctionSet(cmd) => {
            cmd.cmd.execute(&client, &metadata).await;
        }
        SubCommand::Job(cmd) => {
            cmd.cmd.execute(&client, Arc::new(metadata)).await;
        }
        SubCommand::JobResult(cmd) => {
            cmd.cmd.execute(&client, &metadata).await;
        }
        SubCommand::JobStatus(cmd) => {
            cmd.cmd.execute(&client, &metadata).await;
        }
        SubCommand::WorkerInstance(cmd) => {
            cmd.cmd.execute(&client, &metadata).await;
        }
    }
    command_utils::util::tracing::shutdown_tracer_provider();
}
