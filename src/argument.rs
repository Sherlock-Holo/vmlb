use std::time::Duration;

use clap::Parser;
use humantime::parse_duration;

/// vmlb controller
#[derive(Debug, Parser)]
pub struct Argument {
    #[clap(short, long)]
    /// agent grpc service addr
    pub agent_addr: String,

    #[clap(short, long, value_parser(parse_duration), default_value = "60s")]
    /// resync all services interval
    pub resync_interval: Duration,
}
