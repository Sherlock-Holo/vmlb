use std::error::Error;

use clap::Parser;
use http::Uri;
use kube::Client;
use tonic::transport::Channel;
use tracing::info;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::argument::Argument;
use crate::controller::Controller;
use crate::reconcile::GrpcReconcile;

mod argument;
mod controller;
mod reconcile;

pub async fn run() -> Result<(), Box<dyn Error>> {
    let argument = Argument::parse();

    init_log();

    info!(agent_addr = %argument.agent_addr, "connect to agent");

    let kube_client = Client::try_default().await?;
    let channel = Channel::builder(Uri::try_from(&argument.agent_addr)?)
        .connect()
        .await?;

    info!(agent_addr = %argument.agent_addr, "connect to agent done");

    let reconciler = GrpcReconcile::new(channel, kube_client.clone());
    let mut controller = Controller::new(reconciler, kube_client);

    controller.start().await
}

fn init_log() {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(LevelFilter::INFO)
        .init();
}
