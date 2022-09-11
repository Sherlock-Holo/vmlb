use std::io;
use std::net::SocketAddr;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
pub use splice::SpliceProxy;
pub use userspace::UserspaceProxy;

mod splice;
mod userspace;

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Network {
    TCP,
    UDP,
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
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

pub enum EnumProxy {
    Userspace(UserspaceProxy),
    Splice(SpliceProxy),
}

impl From<UserspaceProxy> for EnumProxy {
    fn from(proxy: UserspaceProxy) -> Self {
        Self::Userspace(proxy)
    }
}

impl From<SpliceProxy> for EnumProxy {
    fn from(proxy: SpliceProxy) -> Self {
        Self::Splice(proxy)
    }
}

#[async_trait]
impl Proxy for EnumProxy {
    async fn start_proxy(
        &self,
        listen_addrs: &[String],
        backends: &[SocketAddr],
        network: Network,
    ) -> io::Result<()> {
        match self {
            EnumProxy::Userspace(proxy) => proxy.start_proxy(listen_addrs, backends, network).await,
            EnumProxy::Splice(proxy) => proxy.start_proxy(listen_addrs, backends, network).await,
        }
    }

    async fn stop_proxy(
        &self,
        listen_addrs: &[String],
        backends: &[SocketAddr],
        network: Network,
    ) -> io::Result<()> {
        match self {
            EnumProxy::Userspace(proxy) => proxy.stop_proxy(listen_addrs, backends, network).await,
            EnumProxy::Splice(proxy) => proxy.stop_proxy(listen_addrs, backends, network).await,
        }
    }
}
