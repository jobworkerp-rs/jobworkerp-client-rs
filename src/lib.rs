pub mod jobworkerp {
    pub mod data {
        tonic::include_proto!("jobworkerp.data");
    }
    pub mod service {
        tonic::include_proto!("jobworkerp.service");
    }
}
pub mod client;
pub mod command;
pub mod grpc;
pub mod proto;
