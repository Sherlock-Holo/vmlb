use clap::Parser;

/// vmlb controller
#[derive(Debug, Parser)]
pub struct Argument {
    #[clap(short, long)]
    /// agent grpc service addr
    pub agent_addr: String,
}
