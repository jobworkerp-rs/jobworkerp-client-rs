use anyhow::Result;

pub trait PluginRunner: Send + Sync {
    fn name(&self) -> String;
    fn load(&mut self, settings: Vec<u8>) -> Result<()>;
    fn run(&mut self, arg: Vec<u8>) -> Result<Vec<Vec<u8>>>;
    fn cancel(&mut self) -> bool;
    fn runner_settings_proto(&self) -> String;
    fn job_args_proto(&self) -> String;
    fn result_output_proto(&self) -> Option<String>;
    fn use_job_result(&self) -> bool;
}
