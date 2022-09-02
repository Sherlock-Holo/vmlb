use std::error::Error;
use std::sync::Arc;

use futures_util::{future, StreamExt};
use k8s_openapi::api::core::v1::Service;
use kube::api::ListParams;
use kube::runtime::controller::Action;
use kube::runtime::Controller as RuntimeController;
use kube::{Api, Client};

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
    pub async fn start(&mut self) -> Result<(), Box<dyn Error>> {
        let reconciler = self.reconciler.clone();
        let client = self.client.clone();
        let api = Api::<Service>::all(client);

        RuntimeController::new(api, ListParams::default())
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
