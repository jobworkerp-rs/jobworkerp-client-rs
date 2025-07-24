pub mod jobworkerp {
    pub mod data {
        tonic::include_proto!("jobworkerp.data");
    }
    pub mod service {
        tonic::include_proto!("jobworkerp.service");
    }
    pub mod function {
        pub mod data {
            tonic::include_proto!("jobworkerp.function.data");
        }
        pub mod service {
            tonic::include_proto!("jobworkerp.function.service");
        }
    }
}
pub mod built_in;
pub mod client;
pub mod command;
pub mod display;
pub mod error;
pub mod grpc;
pub mod plugins;
pub mod proto;
