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
use jobworkerp_client::{
    client::JobworkerpClient,
    command::{
        function::FunctionArg, function_set::FunctionSetArg, job::JobArg, job_result::JobResultArg,
        runner::RunnerArg, worker::WorkerArg,
    },
};
use std::time::Duration;

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
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    let opts: Opts = Opts::parse();
    let address = opts.address.clone();
    let timeout = opts.timeout.map(Duration::from_millis);
    let client = JobworkerpClient::new(address, timeout).await.unwrap();
    match opts.subcmd {
        SubCommand::Runner(cmd) => {
            cmd.cmd.execute(&client).await;
        }
        SubCommand::Worker(cmd) => {
            cmd.cmd.execute(&client).await;
        }
        SubCommand::Function(cmd) => {
            cmd.cmd.execute(&client).await;
        }
        SubCommand::FunctionSet(cmd) => {
            cmd.cmd.execute(&client).await;
        }
        SubCommand::Job(cmd) => {
            cmd.cmd.execute(&client).await;
        }
        SubCommand::JobResult(cmd) => {
            cmd.cmd.execute(&client).await;
        }
    }
}
