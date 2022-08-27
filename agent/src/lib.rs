use std::error::Error;
use std::time::Duration;

use api::pb;
use clap::Parser;
use tonic::transport::Server;

use crate::addr_allocate::VethAddrAllocate;
use crate::api::Api;
use crate::argument::Argument;
use crate::persistent::FsPersistent;
use crate::proxy::{Network, UserspaceProxy};

mod addr_allocate;
mod api;
mod argument;
mod persistent;
mod proxy;

const UDP_CONNECTION_TRACK_TIMEOUT: Duration = Duration::from_secs(60 * 5);
const UDP_CONNECTION_TRACK_CHECK_INTERVAL: Duration = Duration::from_secs(5);

pub async fn run() -> Result<(), Box<dyn Error>> {
    let argument = Argument::parse();

    let addr_allocate = VethAddrAllocate::new(&argument.bridge).await?;
    let persistent = FsPersistent::new(argument.persistent_dir).await?;
    let proxy = UserspaceProxy::new(
        UDP_CONNECTION_TRACK_TIMEOUT,
        UDP_CONNECTION_TRACK_CHECK_INTERVAL,
    );

    let api = Api::new(addr_allocate, persistent, proxy);
    let api = pb::agent_server::AgentServer::new(api);

    Server::builder()
        .add_service(api)
        .serve(argument.listen_addr)
        .await?;

    Ok(())
}
