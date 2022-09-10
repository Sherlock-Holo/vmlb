use std::error::Error;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures_util::{future, Stream, StreamExt};
use k8s_openapi::api::core::v1::Service;
use kube::api::ListParams;
use kube::runtime::controller::Action;
use kube::runtime::Controller as RuntimeController;
use kube::{Api, Client};
use tokio::time;
use tokio::time::Interval;

use crate::reconcile::Reconcile;

pub struct Controller<R> {
    reconciler: Arc<R>,
    client: Client,
}

impl<R> Controller<R> {
    pub fn new(reconciler: R, client: Client) -> Self {
        Self {
            reconciler: Arc::new(reconciler),
            client,
        }
    }
}

impl<R> Controller<R>
where
    R: Reconcile + Send + Sync + 'static,
{
    pub async fn start(&mut self, resync_interval: Duration) -> Result<(), Box<dyn Error>> {
        let reconciler = self.reconciler.clone();
        let client = self.client.clone();
        let api = Api::<Service>::all(client);

        RuntimeController::new(api, ListParams::default())
            .reconcile_all_on(IntervalStream::new(resync_interval))
            .run(reconcile, error_policy, reconciler)
            .for_each(|_| future::ready(()))
            .await;

        Ok(())
    }
}

/// The reconciler that will be called when either object change
async fn reconcile<R: Reconcile>(service: Arc<Service>, ctx: Arc<R>) -> Result<Action, R::Error> {
    if service.metadata.deletion_timestamp.is_some() {
        ctx.delete(service).await
    } else {
        ctx.reconcile(service).await
    }
}

/// an error handler that will be called when the reconciler fails
fn error_policy<R: Reconcile>(err: &R::Error, ctx: Arc<R>) -> Action {
    ctx.error_handle(err)
}

struct IntervalStream(Interval);

impl IntervalStream {
    pub fn new(period: Duration) -> Self {
        let start = Instant::now() + period;

        Self(time::interval_at(start.into(), period))
    }
}

impl Stream for IntervalStream {
    type Item = ();

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut interval = Pin::new(&mut self.0);

        futures_util::ready!(interval.poll_tick(cx));

        Poll::Ready(Some(()))
    }
}
