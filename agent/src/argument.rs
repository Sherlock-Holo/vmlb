use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::PathBuf;

use clap::Parser;

/// vmlb agent
#[derive(Parser, Debug)]
#[clap(version, about)]
pub struct Argument {
    #[clap(short, long)]
    /// bridge nic name
    pub bridge: String,

    #[clap(short, long, default_value_os_t = PathBuf::from("/run/vmlb"))]
    /// record persistent dir
    pub persistent_dir: PathBuf,

    #[clap(short, long, default_value_t = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::from([0, 0, 0, 0]), 8599)))]
    /// grpc service listen addr
    pub listen_addr: SocketAddr,
}
