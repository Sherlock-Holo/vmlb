use std::io;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
pub use userspace::UserspaceProxy;

mod userspace;

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Network {
    TCP,
    UDP,
}

#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub trait Proxy {
    async fn start_proxy(
        &self,
        listen_addrs: &[String],
        backends: &[SocketAddr],
        network: Network,
    ) -> io::Result<()>;

    async fn stop_proxy(
        &self,
        listen_addrs: &[String],
        backends: &[SocketAddr],
        network: Network,
    ) -> io::Result<()>;
}
