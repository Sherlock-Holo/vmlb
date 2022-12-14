use std::io::ErrorKind;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;

use pb::agent_server::Agent;
use pb::{AddServiceRequest, AddServiceResponse, DeleteServiceRequest, DeleteServiceResponse};
use tap::TapFallible;
use tonic::{Request, Response, Status};
use tracing::{error, info, warn};

use crate::addr_allocate::AddrAllocate;
use crate::api::pb::Protocol;
use crate::persistent::{Forward, Persistent, Record};
use crate::proxy::{Network, Proxy};

// prost doesn't want to add it, so we have to add by ourself
#[allow(clippy::derive_partial_eq_without_eq)]
pub mod pb {
    tonic::include_proto!("vmlb.agent");
}

pub struct Api<Al, Pe, Pr> {
    inner: Arc<ApiInner<Al, Pe, Pr>>,
}

impl<Al, Pe, Pr> Clone for Api<Al, Pe, Pr> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

struct ApiInner<Al, Pe, Pr> {
    addr_allocate: Al,
    persistent: Pe,
    proxy: Pr,
}

impl<Al, Pe, Pr> Api<Al, Pe, Pr> {
    pub fn new(addr_allocate: Al, persistent: Pe, proxy: Pr) -> Self {
        Self {
            inner: Arc::new(ApiInner {
                addr_allocate,
                persistent,
                proxy,
            }),
        }
    }

    fn add_service_check(&self, req: &AddServiceRequest) -> Result<(), Status> {
        if req.namespace.is_empty() {
            error!("empty namespace");

            return Err(Status::invalid_argument("empty namespace"));
        }

        if req.service.is_empty() {
            error!("empty service");

            return Err(Status::invalid_argument("empty service"));
        }

        if req.forwards.is_empty() {
            error!("empty forwards");

            return Err(Status::invalid_argument("empty forwards"));
        }

        for forward in req.forwards.iter() {
            if forward.protocol != Protocol::Tcp as i32 && forward.protocol != Protocol::Udp as i32
            {
                error!(protocol = forward.protocol, "invalid protocol");

                return Err(Status::invalid_argument(format!(
                    "invalid protocol {}",
                    forward.protocol
                )));
            }

            if forward.port > 65535 || forward.port == 0 {
                error!(port = forward.port, "invalid port");

                return Err(Status::invalid_argument(format!(
                    "invalid port {}",
                    forward.port
                )));
            }

            for backend in forward.backends.iter() {
                if let Err(err) = backend.addr.parse::<IpAddr>() {
                    error!(%err, addr = %backend.addr, "invalid addr");

                    return Err(Status::invalid_argument(format!(
                        "invalid addr {}",
                        backend.addr
                    )));
                }

                if backend.target_port > 65535 || backend.target_port == 0 {
                    error!(target_port = backend.target_port, "invalid target_port");

                    return Err(Status::invalid_argument(format!(
                        "invalid target_port {}",
                        backend.target_port
                    )));
                }
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl<Al, Pe, Pr> Agent for Api<Al, Pe, Pr>
where
    Al: AddrAllocate + Send + Sync + 'static,
    Pe: Persistent + Send + Sync + 'static,
    Pr: Proxy + Send + Sync + 'static,
    Pe::Error: Send + Sync,
{
    async fn add_service(
        &self,
        request: Request<AddServiceRequest>,
    ) -> Result<Response<AddServiceResponse>, Status> {
        let request = request.into_inner();

        self.add_service_check(&request)?;

        info!(?request, "argument check done");

        let endpoints = match self
            .inner
            .addr_allocate
            .allocate(&request.namespace, &request.service)
            .await
        {
            Err(err) => {
                return Err(Status::internal(err.to_string()));
            }

            Ok(endpoints) => endpoints,
        };

        info!(namespace = %request.namespace, service = %request.service, ?endpoints, "allocate addr done");

        let forwards = request
            .forwards
            .into_iter()
            .map(|forward| {
                let protocol = if forward.protocol == Protocol::Tcp as i32 {
                    Network::TCP
                } else if forward.protocol == Protocol::Udp as i32 {
                    Network::UDP
                } else {
                    // add_service_check has checked
                    unreachable!()
                };

                let backends = forward
                    .backends
                    .iter()
                    .map(|backend| format!("{}:{}", backend.addr, backend.target_port))
                    .filter_map(|addr| {
                        SocketAddr::from_str(&addr)
                            .tap_err(|err| warn!(%err, %addr, "ignore invalid addr"))
                            .ok()
                    })
                    .collect::<Vec<_>>();

                Forward {
                    endpoints: endpoints.clone(),
                    port: forward.port as _,
                    protocol,
                    backends,
                }
            })
            .collect::<Vec<_>>();

        for forward in forwards.iter() {
            let endpoints = forward
                .endpoints
                .iter()
                .map(|ep| format!("{}:{}", ep, forward.port))
                .collect::<Vec<_>>();

            if let Err(err) = self
                .inner
                .proxy
                .start_proxy(&endpoints, &forward.backends, forward.protocol)
                .await
            {
                if err.kind() == ErrorKind::AddrInUse {
                    info!(?forward, "forward proxy exists");

                    continue;
                }

                return Err(Status::internal(err.to_string()));
            }

            info!(endpoints = ?forward.endpoints, backends = ?forward.backends, protocol = ?forward.protocol, "start proxy done");
        }

        if let Err(err) = self
            .inner
            .persistent
            .store_record(&request.namespace, &request.service, Record { forwards })
            .await
        {
            return Err(Status::internal(err.to_string()));
        }

        info!(namespace = %request.namespace, service = %request.service, "store record done");

        Ok(Response::new(AddServiceResponse { addr: endpoints }))
    }

    async fn delete_service(
        &self,
        request: Request<DeleteServiceRequest>,
    ) -> Result<Response<DeleteServiceResponse>, Status> {
        let request = request.into_inner();

        if let Err(err) = self
            .inner
            .addr_allocate
            .deallocate(&request.namespace, &request.service)
            .await
        {
            return Err(Status::internal(err.to_string()));
        }

        info!(namespace = %request.namespace, service = %request.service, "deallocate ip for service done");

        let record = match self
            .inner
            .persistent
            .load_record(&request.namespace, &request.service)
            .await
        {
            Err(err) => {
                return Err(Status::internal(err.to_string()));
            }

            Ok(None) => {
                warn!(namespace = %request.namespace, service = %request.service, "record not found");

                return Ok(Response::new(DeleteServiceResponse {}));
            }

            Ok(Some(record)) => record,
        };

        for forward in record.forwards {
            let endpoints = forward
                .endpoints
                .iter()
                .map(|ep| format!("{}:{}", ep, forward.port))
                .collect::<Vec<_>>();

            if let Err(err) = self
                .inner
                .proxy
                .stop_proxy(&endpoints, &forward.backends, forward.protocol)
                .await
            {
                warn!(
                    %err,
                    namespace = %request.namespace,
                    service = %request.service,
                    endpoints = ?forward.endpoints,
                    backends = ?forward.backends,
                    protocol = ?forward.protocol,
                    "stop proxy failed"
                );
            }
        }

        if let Err(err) = self
            .inner
            .persistent
            .delete_record(&request.namespace, &request.service)
            .await
        {
            Err(Status::internal(err.to_string()))
        } else {
            info!(namespace = %request.namespace, service = %request.service, "delete record for service done");

            Ok(Response::new(DeleteServiceResponse {}))
        }
    }
}

#[cfg(test)]
mod tests {
    use mockall::predicate::*;

    use super::*;
    use crate::addr_allocate::MockAddrAllocate;
    use crate::api::pb::Backend;
    use crate::persistent::MockPersistent;
    use crate::proxy::MockProxy;

    #[test]
    fn test_new() {
        let addr_allocate = MockAddrAllocate::new();
        let persistent = MockPersistent::new();
        let proxy = MockProxy::new();

        Api::new(addr_allocate, persistent, proxy);
    }

    #[tokio::test]
    async fn test_delete_service() {
        let mut addr_allocate = MockAddrAllocate::new();
        let mut persistent = MockPersistent::new();
        let mut proxy = MockProxy::new();

        addr_allocate
            .expect_deallocate()
            .with(eq("default"), eq("test"))
            .returning(|_, _| Ok(()));
        persistent
            .expect_load_record()
            .with(eq("default"), eq("test"))
            .returning(|_, _| {
                Ok(Some(Record {
                    forwards: vec![Forward {
                        endpoints: vec!["127.0.0.1".to_string()],
                        port: 80,
                        protocol: Network::TCP,
                        backends: vec!["1.1.1.1:80".parse().unwrap()],
                    }],
                }))
            });
        proxy
            .expect_stop_proxy()
            .withf(|listen_addr, backends, network| {
                listen_addr == ["127.0.0.1:80".to_string()].as_slice()
                    && backends == ["1.1.1.1:80".parse().unwrap()].as_slice()
                    && *network == Network::TCP
            })
            .returning(|_, _, _| Ok(()));
        persistent
            .expect_delete_record()
            .with(eq("default"), eq("test"))
            .returning(|_, _| Ok(()));

        let api = Api::new(addr_allocate, persistent, proxy);

        api.delete_service(Request::new(DeleteServiceRequest {
            namespace: "default".to_string(),
            service: "test".to_string(),
        }))
        .await
        .unwrap();
    }

    #[tokio::test]
    #[should_panic]
    async fn test_add_service_empty_ns() {
        let addr_allocate = MockAddrAllocate::new();
        let persistent = MockPersistent::new();
        let proxy = MockProxy::new();

        let api = Api::new(addr_allocate, persistent, proxy);

        api.add_service(Request::new(AddServiceRequest {
            namespace: "".to_string(),
            service: "".to_string(),
            forwards: vec![],
        }))
        .await
        .unwrap();
    }

    #[tokio::test]
    #[should_panic]
    async fn test_add_service_empty_svc() {
        let addr_allocate = MockAddrAllocate::new();
        let persistent = MockPersistent::new();
        let proxy = MockProxy::new();

        let api = Api::new(addr_allocate, persistent, proxy);

        api.add_service(Request::new(AddServiceRequest {
            namespace: "default".to_string(),
            service: "".to_string(),
            forwards: vec![],
        }))
        .await
        .unwrap();
    }

    #[tokio::test]
    #[should_panic]
    async fn test_add_service_empty_forward() {
        let addr_allocate = MockAddrAllocate::new();
        let persistent = MockPersistent::new();
        let proxy = MockProxy::new();

        let api = Api::new(addr_allocate, persistent, proxy);

        api.add_service(Request::new(AddServiceRequest {
            namespace: "default".to_string(),
            service: "test".to_string(),
            forwards: vec![],
        }))
        .await
        .unwrap();
    }

    #[tokio::test]
    #[should_panic]
    async fn test_add_service_invalid_protocol() {
        let addr_allocate = MockAddrAllocate::new();
        let persistent = MockPersistent::new();
        let proxy = MockProxy::new();

        let api = Api::new(addr_allocate, persistent, proxy);

        api.add_service(Request::new(AddServiceRequest {
            namespace: "default".to_string(),
            service: "test".to_string(),
            forwards: vec![pb::Forward {
                protocol: 100,
                port: 0,
                backends: vec![],
            }],
        }))
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_add_service_invalid_port() {
        let addr_allocate = MockAddrAllocate::new();
        let persistent = MockPersistent::new();
        let proxy = MockProxy::new();

        let api = Api::new(addr_allocate, persistent, proxy);

        api.add_service(Request::new(AddServiceRequest {
            namespace: "default".to_string(),
            service: "test".to_string(),
            forwards: vec![pb::Forward {
                protocol: Protocol::Tcp as _,
                port: 0,
                backends: vec![],
            }],
        }))
        .await
        .unwrap_err();

        api.add_service(Request::new(AddServiceRequest {
            namespace: "default".to_string(),
            service: "test".to_string(),
            forwards: vec![pb::Forward {
                protocol: Protocol::Tcp as _,
                port: 70000,
                backends: vec![],
            }],
        }))
        .await
        .unwrap_err();
    }

    #[tokio::test]
    #[should_panic]
    async fn test_add_service_invalid_backend_addr() {
        let addr_allocate = MockAddrAllocate::new();
        let persistent = MockPersistent::new();
        let proxy = MockProxy::new();

        let api = Api::new(addr_allocate, persistent, proxy);

        api.add_service(Request::new(AddServiceRequest {
            namespace: "default".to_string(),
            service: "test".to_string(),
            forwards: vec![pb::Forward {
                protocol: Protocol::Tcp as _,
                port: 80,
                backends: vec![Backend {
                    addr: "123".to_string(),
                    target_port: 0,
                }],
            }],
        }))
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_add_service_invalid_target_port() {
        let addr_allocate = MockAddrAllocate::new();
        let persistent = MockPersistent::new();
        let proxy = MockProxy::new();

        let api = Api::new(addr_allocate, persistent, proxy);

        api.add_service(Request::new(AddServiceRequest {
            namespace: "default".to_string(),
            service: "test".to_string(),
            forwards: vec![pb::Forward {
                protocol: Protocol::Tcp as _,
                port: 80,
                backends: vec![Backend {
                    addr: "127.0.0.1".to_string(),
                    target_port: 0,
                }],
            }],
        }))
        .await
        .unwrap_err();

        api.add_service(Request::new(AddServiceRequest {
            namespace: "default".to_string(),
            service: "test".to_string(),
            forwards: vec![pb::Forward {
                protocol: Protocol::Tcp as _,
                port: 80,
                backends: vec![Backend {
                    addr: "127.0.0.1".to_string(),
                    target_port: 70000,
                }],
            }],
        }))
        .await
        .unwrap_err();
    }

    #[tokio::test]
    async fn test_add_service() {
        let mut addr_allocate = MockAddrAllocate::new();
        let mut persistent = MockPersistent::new();
        let mut proxy = MockProxy::new();

        addr_allocate
            .expect_allocate()
            .with(eq("default"), eq("test"))
            .returning(|_, _| Ok(vec!["127.0.0.1".to_string()]));
        proxy
            .expect_start_proxy()
            .withf(|listen_addr, backends, network| {
                listen_addr == ["127.0.0.1:80".to_string()].as_slice()
                    && backends == ["1.1.1.1:80".parse().unwrap()].as_slice()
                    && *network == Network::TCP
            })
            .returning(|_, _, _| Ok(()));
        persistent
            .expect_store_record()
            .with(
                eq("default"),
                eq("test"),
                eq(Record {
                    forwards: vec![Forward {
                        endpoints: vec!["127.0.0.1".to_string()],
                        port: 80,
                        protocol: Network::TCP,
                        backends: vec!["1.1.1.1:80".parse().unwrap()],
                    }],
                }),
            )
            .returning(|_, _, _| Ok(()));

        let api = Api::new(addr_allocate, persistent, proxy);

        api.add_service(Request::new(AddServiceRequest {
            namespace: "default".to_string(),
            service: "test".to_string(),
            forwards: vec![pb::Forward {
                protocol: Protocol::Tcp as _,
                port: 80,
                backends: vec![Backend {
                    addr: "1.1.1.1".to_string(),
                    target_port: 80,
                }],
            }],
        }))
        .await
        .unwrap();
    }
}
