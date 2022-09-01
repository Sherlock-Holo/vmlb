use std::error::Error;
use std::sync::Arc;

use k8s_openapi::api::core::v1::Service;
use kube::runtime::controller::Action;

mod grpc_reconcile;

#[cfg_attr(test, mockall::automock(type Error = std::io::Error;))]
#[async_trait::async_trait]
pub trait Reconcile {
    type Error: Error + Send;

    async fn reconcile(&self, service: Arc<Service>) -> Result<Action, Self::Error>;

    async fn delete(&self, service: Arc<Service>) -> Result<Action, Self::Error>;

    fn error_handle(&self, err: &Self::Error) -> Action;
}
