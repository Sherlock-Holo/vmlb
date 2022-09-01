use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{Request, Response};
use http_body::Body;
use k8s_openapi::api::core::v1::{
    LoadBalancerIngress, LoadBalancerStatus, Node, Service, ServicePort, ServiceStatus,
};
use kube::api::{ListParams, Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::{Api, Client};
use pb::agent_client::AgentClient;
use pb::Forward;
use serde_json::Value;
use tap::TapFallible;
use thiserror::Error;
use tonic::body::BoxBody;
use tonic::Status;
use tower_service::Service as TowerService;
use tracing::{error, info, warn};

use crate::reconcile::grpc_reconcile::pb::{
    AddServiceRequest, Backend, DeleteServiceRequest, Protocol,
};
use crate::reconcile::Reconcile;

// prost doesn't want to add it, so we have to add by ourself
#[allow(clippy::derive_partial_eq_without_eq)]
mod pb {
    tonic::include_proto!("vmlb.agent");
}

const VMLB_ANNOTATION_PORTS_KEY: &str = "vmlb/last-ports-config";
const VMLB_FINALIZER: &str = "vmlb-finalizer";
const REQUEUE: Duration = Duration::from_secs(3);

#[derive(Debug, Error)]
pub enum Error {
    #[error("service not found")]
    NotFound,

    #[error("agent error: {0}")]
    Grpc(#[from] Status),

    #[error("serde json error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("kube api error: {0}")]
    Kube(#[from] kube::Error),
}

pub struct GrpcReconcile<S> {
    agent_client: AgentClient<S>,
    kube_client: Client,
}

impl<S> GrpcReconcile<S> {
    async fn get_node_addrs(&self) -> Result<Vec<IpAddr>, Error> {
        let node_api = Api::<Node>::all(self.kube_client.clone());

        let nodes = node_api
            .list(&ListParams::default())
            .await
            .tap_err(|err| error!(%err, "list all nodes failed"))?;

        let node_addrs = nodes
            .into_iter()
            .filter_map(|node| node.status)
            .filter_map(|node| node.addresses)
            .flatten()
            .filter_map(|node_addr| {
                if node_addr.type_ == "InternalIP" {
                    Some(node_addr.address)
                } else {
                    None
                }
            })
            .filter_map(|node_addr| {
                node_addr
                    .parse()
                    .tap_err(|err| warn!(%err, "ignore invalid node addr"))
                    .ok()
            })
            .collect::<Vec<_>>();

        Ok(node_addrs)
    }
}

impl<S, RespBody> GrpcReconcile<S>
where
    S: TowerService<Request<BoxBody>, Response = Response<RespBody>> + Send + Sync,
    S::Error: Into<Box<dyn StdError + Send + Sync>>,
    S: Clone,
    S::Future: Send,
    RespBody: Body<Data = Bytes> + Send + 'static,
    RespBody::Error: Into<Box<dyn StdError + Send + Sync>> + Send,
{
    pub fn new(service: S, kube_client: Client) -> Self {
        Self {
            agent_client: AgentClient::new(service),
            kube_client,
        }
    }

    async fn add_service(
        &self,
        namespace: &str,
        name: &str,
        ports: &[ServicePort],
        annotations: &mut BTreeMap<String, String>,
        node_addrs: &[IpAddr],
        finalizers: Vec<String>,
    ) -> Result<Action, Error> {
        let ports_config = serde_json::to_string(ports)
            .tap_err(|err| error!(%err, ?ports, "marshal ports failed"))?;

        info!(?ports, %ports_config, "marshal ports done");

        annotations.insert(VMLB_ANNOTATION_PORTS_KEY.to_string(), ports_config);

        let forwards = ports
            .iter()
            .map(|port| {
                let node_port = port.node_port.unwrap();
                let backends = node_addrs
                    .iter()
                    .map(|addr| Backend {
                        addr: addr.to_string(),
                        target_port: node_port as _,
                    })
                    .collect();

                Forward {
                    protocol: match port.protocol.as_deref() {
                        Some("UDP") => Protocol::Udp as i32,
                        _ => Protocol::Tcp as i32,
                    },
                    port: port.port as _,
                    backends,
                }
            })
            .collect::<Vec<_>>();

        info!(?forwards, "collect forwards done");

        let add_req = AddServiceRequest {
            namespace: namespace.to_string(),
            service: name.to_string(),
            forwards,
        };

        let resp = self
            .agent_client
            .clone()
            .add_service(add_req)
            .await
            .tap_err(|err| error!(%err, "add service failed"))?;
        let resp = resp.into_inner();

        info!(?resp, "add service done");

        let lb_ingress_list = resp
            .addr
            .into_iter()
            .map(|addr| LoadBalancerIngress {
                hostname: None,
                ip: Some(addr),
                ports: None,
            })
            .collect::<Vec<_>>();

        let status = ServiceStatus {
            conditions: None,
            load_balancer: Some(LoadBalancerStatus {
                ingress: Some(lb_ingress_list),
            }),
        };

        let patch_content = serde_json::json!({
            "metadata": {
                "annotations": annotations,
                "finalizers": finalizers
            },
            "status": status
        });
        let patch = Patch::Apply(patch_content);

        let api = Api::<Service>::namespaced(self.kube_client.clone(), namespace);

        api.patch(name, &PatchParams::default(), &patch)
            .await
            .tap_err(|err| error!(namespace, name, ?patch, %err, "patch service failed"))?;

        info!(namespace, name, ?patch, "patch service done");

        Ok(Action::await_change())
    }

    async fn delete_service(&self, namespace: &str, name: &str) -> Result<(), Error> {
        self.agent_client
            .clone()
            .delete_service(DeleteServiceRequest {
                namespace: namespace.to_string(),
                service: name.to_string(),
            })
            .await
            .tap_err(|err| error!(namespace, name, %err, "delete service failed"))?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl<S, RespBody> Reconcile for GrpcReconcile<S>
where
    S: TowerService<Request<BoxBody>, Response = Response<RespBody>> + Send + Sync,
    S::Error: Into<Box<dyn StdError + Send + Sync>>,
    S: Clone,
    S::Future: Send,
    RespBody: Body<Data = Bytes> + Send + 'static,
    RespBody::Error: Into<Box<dyn StdError + Send + Sync>> + Send,
{
    type Error = Error;

    async fn reconcile(&self, service: Arc<Service>) -> Result<Action, Self::Error> {
        let namespace = service.metadata.namespace.as_deref().unwrap_or("default");
        let name = match service.metadata.name.as_deref() {
            None => {
                error!(namespace, "service has no name");

                return Err(Error::NotFound);
            }

            Some(name) => name,
        };

        let mut annotations = service
            .metadata
            .annotations
            .as_ref()
            .cloned()
            .unwrap_or_default();
        let mut finalizers = service
            .metadata
            .finalizers
            .as_ref()
            .cloned()
            .unwrap_or_default();

        let finalizer_mark = VMLB_FINALIZER.to_string();

        if !finalizers.contains(&finalizer_mark) {
            finalizers.push(finalizer_mark);
        }

        let spec = match service.spec.as_ref() {
            None => {
                error!(namespace, name, "ignore empty spec service");

                return Err(Error::NotFound);
            }

            Some(spec) => spec,
        };

        if !matches!(spec.type_.as_deref(), Some("LoadBalancer")) {
            info!(namespace, name, "ignore not lb service");

            return Ok(Action::await_change());
        }

        let ports = match &spec.ports {
            None => {
                info!(namespace, name, "ignore empty port services");

                return Ok(Action::await_change());
            }

            Some(ports) if ports.is_empty() => {
                info!(namespace, name, "ignore empty port services");

                return Ok(Action::await_change());
            }

            Some(ports) => ports,
        };

        if ports.iter().any(|port| port.node_port.is_none()) {
            info!(
                namespace,
                name, "service still has no node port, requeue it"
            );

            return Ok(Action::requeue(Duration::from_secs(3)));
        }

        let node_addrs = self.get_node_addrs().await?;

        info!(?node_addrs, "get node addrs done");

        match annotations.get(VMLB_ANNOTATION_PORTS_KEY) {
            None => {
                self.add_service(
                    namespace,
                    name,
                    ports,
                    &mut annotations,
                    &node_addrs,
                    finalizers,
                )
                .await
            }

            Some(last_port_config) => {
                match serde_json::from_str::<Vec<ServicePort>>(last_port_config) {
                    Err(err) => {
                        warn!(namespace, name, %err, "parse last port config failed");
                    }

                    Ok(mut last_port_config) => {
                        if last_port_config.len() == ports.len() {
                            // clone so we can sort it
                            let mut ports = ports.clone();

                            last_port_config.sort_by(|a, b| a.port.cmp(&b.port));
                            ports.sort_by(|a, b| a.port.cmp(&b.port));

                            if last_port_config == ports {
                                info!(namespace, name, "ignore unchanged service");

                                return Ok(Action::await_change());
                            }
                        }
                    }
                }

                self.delete_service(namespace, name).await?;
                self.add_service(
                    namespace,
                    name,
                    ports,
                    &mut annotations,
                    &node_addrs,
                    finalizers,
                )
                .await
            }
        }
    }

    async fn delete(&self, service: Arc<Service>) -> Result<Action, Self::Error> {
        let namespace = service.metadata.namespace.as_deref().unwrap_or("default");
        let name = match service.metadata.name.as_deref() {
            None => {
                error!(namespace, "service has no name");

                return Err(Error::NotFound);
            }

            Some(name) => name,
        };

        self.delete_service(namespace, name).await?;

        info!(namespace, name, "delete service done");

        let mut patch = if let Some(finalizers) = service.metadata.finalizers.as_ref() {
            let finalizer_mark = VMLB_FINALIZER.to_string();
            if finalizers.contains(&finalizer_mark) {
                let mut finalizers = finalizers.clone();
                if let Some(index) = finalizers
                    .iter()
                    .position(|finalizer| *finalizer == finalizer_mark)
                {
                    finalizers.swap_remove(index);

                    serde_json::json!({
                        "metadata": {
                            "finalizers": finalizers
                        }
                    })
                } else {
                    serde_json::json!({ "metadata": {} })
                }
            } else {
                serde_json::json!({ "metadata": {} })
            }
        } else {
            serde_json::json!({ "metadata": {} })
        };

        let mut annotations = service
            .metadata
            .annotations
            .as_ref()
            .cloned()
            .unwrap_or_default();
        annotations.remove(VMLB_ANNOTATION_PORTS_KEY);

        patch
            .get_mut("metadata")
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert(
                "annotations".to_string(),
                serde_json::to_value(annotations)
                    .tap_err(|err| error!(%err, "marshal annotations failed"))?,
            );

        if let Some(status) = &service.status {
            if status.load_balancer.is_some() {
                let mut status = status.clone();
                status.load_balancer.take();
                patch.as_object_mut().unwrap().insert(
                    "status".to_string(),
                    serde_json::to_value(status)
                        .tap_err(|err| error!(%err, "marshal status failed"))?,
                );
            }
        }
        let patch = Patch::Apply(patch);

        Api::<Service>::namespaced(self.kube_client.clone(), namespace)
            .patch(name, &PatchParams::default(), &patch)
            .await
            .tap_err(|err| error!(namespace, name, ?patch, %err, "patch finalizers failed"))?;

        Ok(Action::await_change())
    }

    fn error_handle(&self, err: &Self::Error) -> Action {
        match err {
            Error::NotFound => Action::await_change(),
            Error::Grpc(status) => {
                error!(%status, "reconcile call agent failed");

                Action::requeue(REQUEUE)
            }
            Error::Serde(err) => {
                error!(%err, "reconcile serde failed");

                Action::requeue(REQUEUE)
            }
            Error::Kube(err) => {
                if matches!(err, kube::Error::Api(err_resp) if err_resp.code==http::StatusCode::NOT_FOUND.as_u16())
                {
                    Action::await_change()
                } else {
                    error!(%err, "reconcile failed");

                    Action::requeue(REQUEUE)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use bytes::Buf;
    use futures_util::stream;
    use http::{Method, Uri};
    use hyper::body::aggregate;
    use k8s_openapi::api::core::v1::{NodeAddress, NodeStatus, ServiceSpec};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
    use kube::api::ObjectList;
    use serde_json::Value;
    use tonic::transport::{Endpoint, Server};
    use tonic::Streaming;
    use tower_test::mock;

    use super::*;
    use crate::reconcile::grpc_reconcile::pb::agent_server::{Agent, AgentServer};
    use crate::reconcile::grpc_reconcile::pb::{AddServiceResponse, DeleteServiceResponse};

    #[tokio::test]
    async fn not_lb() {
        let (k8s_service, _k8s_handle) = mock::pair::<_, Response<hyper::Body>>();
        let (grpc_service, _grpc_handle) = mock::pair::<_, Response<BoxBody>>();
        let k8s_client = Client::new(k8s_service, "default");
        let grpc_reconciler = GrpcReconcile::new(grpc_service, k8s_client);

        let service = Arc::new(Service {
            metadata: ObjectMeta {
                namespace: Some("default".to_string()),
                name: Some("test".to_string()),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                type_: Some("NodePort".to_string()),
                ..Default::default()
            }),
            status: None,
        });

        dbg!(grpc_reconciler.reconcile(service).await.unwrap());
    }

    #[tokio::test]
    async fn empty_port() {
        let (k8s_service, _k8s_handle) = mock::pair::<_, Response<hyper::Body>>();
        let (grpc_service, _grpc_handle) = mock::pair::<_, Response<BoxBody>>();
        let k8s_client = Client::new(k8s_service, "default");
        let grpc_reconciler = GrpcReconcile::new(grpc_service, k8s_client);

        let service = Arc::new(Service {
            metadata: ObjectMeta {
                namespace: Some("default".to_string()),
                name: Some("test".to_string()),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                type_: Some("LoadBalancer".to_string()),
                ..Default::default()
            }),
            status: None,
        });

        dbg!(grpc_reconciler.reconcile(service).await.unwrap());
    }

    #[tokio::test]
    async fn no_node_port() {
        let (k8s_service, _k8s_handle) = mock::pair::<_, Response<hyper::Body>>();
        let (grpc_service, _grpc_handle) = mock::pair::<_, Response<BoxBody>>();
        let k8s_client = Client::new(k8s_service, "default");
        let grpc_reconciler = GrpcReconcile::new(grpc_service, k8s_client);

        let service = Arc::new(Service {
            metadata: ObjectMeta {
                namespace: Some("default".to_string()),
                name: Some("test".to_string()),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                type_: Some("LoadBalancer".to_string()),
                ports: Some(vec![ServicePort {
                    port: 80,
                    protocol: Some("TCP".to_string()),
                    target_port: Some(IntOrString::Int(80)),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            status: None,
        });

        dbg!(grpc_reconciler.reconcile(service).await.unwrap());
    }

    #[tokio::test]
    async fn add_new_service() {
        let (k8s_service, mut k8s_handle) = mock::pair::<_, Response<hyper::Body>>();

        struct TestAgentServer;

        #[async_trait]
        impl Agent for TestAgentServer {
            type RefreshServicesStream = stream::Empty<Result<AddServiceResponse, Status>>;

            async fn refresh_services(
                &self,
                _request: tonic::Request<Streaming<AddServiceRequest>>,
            ) -> Result<tonic::Response<Self::RefreshServicesStream>, Status> {
                unreachable!()
            }

            async fn add_service(
                &self,
                request: tonic::Request<AddServiceRequest>,
            ) -> Result<tonic::Response<AddServiceResponse>, Status> {
                assert_eq!(
                    request.into_inner(),
                    AddServiceRequest {
                        namespace: "default".to_string(),
                        service: "test".to_string(),
                        forwards: vec![Forward {
                            protocol: Protocol::Tcp as i32,
                            port: 80,
                            backends: vec![Backend {
                                addr: "127.0.0.1".to_string(),
                                target_port: 36666,
                            }],
                        }],
                    }
                );

                Ok(tonic::Response::new(AddServiceResponse {
                    addr: vec!["192.168.1.1".to_string()],
                }))
            }

            async fn delete_service(
                &self,
                _request: tonic::Request<DeleteServiceRequest>,
            ) -> Result<tonic::Response<DeleteServiceResponse>, Status> {
                unreachable!()
            }
        }

        let (client, server) = tokio::io::duplex(4096);
        let mut client = Some(client);

        tokio::spawn(async move {
            Server::builder()
                .add_service(AgentServer::new(TestAgentServer))
                .serve_with_incoming(stream::iter(vec![Ok::<_, std::io::Error>(server)]))
                .await
        });

        let k8s_client = Client::new(k8s_service, "default");

        let channel = Endpoint::try_from("http://127.0.0.1:80")
            .unwrap()
            .connect_with_connector(tower::service_fn(move |_: Uri| {
                let client = client.take();

                async move {
                    match client {
                        None => Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "Client already taken",
                        )),
                        Some(client) => Ok(client),
                    }
                }
            }))
            .await
            .unwrap();

        let grpc_reconciler = GrpcReconcile::new(channel, k8s_client);

        tokio::spawn(async move {
            let (request, send) = k8s_handle.next_request().await.unwrap();
            assert_eq!(request.method(), Method::GET);
            assert_eq!(request.uri().to_string(), "/api/v1/nodes?");

            let node = Node {
                metadata: Default::default(),
                spec: None,
                status: Some(NodeStatus {
                    addresses: Some(vec![NodeAddress {
                        type_: "InternalIP".to_string(),
                        address: "127.0.0.1".to_string(),
                    }]),
                    ..Default::default()
                }),
            };

            send.send_response(Response::new(hyper::Body::from(
                serde_json::to_string(&ObjectList {
                    metadata: Default::default(),
                    items: vec![node],
                })
                .unwrap(),
            )));

            let (request, send) = k8s_handle.next_request().await.unwrap();
            assert_eq!(request.method(), Method::PATCH);
            assert_eq!(
                request.uri().to_string(),
                "/api/v1/namespaces/default/services/test?"
            );

            let service = Service {
                metadata: ObjectMeta {
                    namespace: Some("default".to_string()),
                    name: Some("test".to_string()),
                    finalizers: Some(vec![VMLB_FINALIZER.to_string()]),
                    ..Default::default()
                },
                spec: Some(ServiceSpec {
                    type_: Some("LoadBalancer".to_string()),
                    ports: Some(vec![ServicePort {
                        port: 80,
                        protocol: Some("TCP".to_string()),
                        target_port: Some(IntOrString::Int(80)),
                        node_port: Some(36666),
                        ..Default::default()
                    }]),
                    ..Default::default()
                }),
                status: Some(ServiceStatus {
                    load_balancer: Some(LoadBalancerStatus {
                        ingress: Some(vec![LoadBalancerIngress {
                            hostname: None,
                            ip: Some("192.168.1.1".to_string()),
                            ports: None,
                        }]),
                    }),
                    ..Default::default()
                }),
            };

            send.send_response(Response::new(hyper::Body::from(
                serde_json::to_string(&service).unwrap(),
            )));
        });

        let service = Arc::new(Service {
            metadata: ObjectMeta {
                namespace: Some("default".to_string()),
                name: Some("test".to_string()),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                type_: Some("LoadBalancer".to_string()),
                ports: Some(vec![ServicePort {
                    port: 80,
                    protocol: Some("TCP".to_string()),
                    target_port: Some(IntOrString::Int(80)),
                    node_port: Some(36666),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            status: None,
        });

        dbg!(grpc_reconciler.reconcile(service).await.unwrap());
    }

    #[tokio::test]
    async fn update_exists_service() {
        let (k8s_service, mut k8s_handle) = mock::pair::<_, Response<hyper::Body>>();

        struct TestAgentServer;

        #[async_trait]
        impl Agent for TestAgentServer {
            type RefreshServicesStream = stream::Empty<Result<AddServiceResponse, Status>>;

            async fn refresh_services(
                &self,
                request: tonic::Request<Streaming<AddServiceRequest>>,
            ) -> Result<tonic::Response<Self::RefreshServicesStream>, Status> {
                unreachable!()
            }

            async fn add_service(
                &self,
                request: tonic::Request<AddServiceRequest>,
            ) -> Result<tonic::Response<AddServiceResponse>, Status> {
                assert_eq!(
                    request.into_inner(),
                    AddServiceRequest {
                        namespace: "default".to_string(),
                        service: "test".to_string(),
                        forwards: vec![Forward {
                            protocol: Protocol::Tcp as i32,
                            port: 8800,
                            backends: vec![Backend {
                                addr: "127.0.0.1".to_string(),
                                target_port: 36666,
                            }],
                        }],
                    }
                );

                Ok(tonic::Response::new(AddServiceResponse {
                    addr: vec!["192.168.1.2".to_string()],
                }))
            }

            async fn delete_service(
                &self,
                request: tonic::Request<DeleteServiceRequest>,
            ) -> Result<tonic::Response<DeleteServiceResponse>, Status> {
                assert_eq!(
                    request.into_inner(),
                    DeleteServiceRequest {
                        namespace: "default".to_string(),
                        service: "test".to_string(),
                    }
                );

                Ok(tonic::Response::new(DeleteServiceResponse {}))
            }
        }

        let (client, server) = tokio::io::duplex(4096);
        let mut client = Some(client);

        tokio::spawn(async move {
            Server::builder()
                .add_service(AgentServer::new(TestAgentServer))
                .serve_with_incoming(stream::iter(vec![Ok::<_, std::io::Error>(server)]))
                .await
        });

        let k8s_client = Client::new(k8s_service, "default");

        let channel = Endpoint::try_from("http://127.0.0.1:80")
            .unwrap()
            .connect_with_connector(tower::service_fn(move |_: Uri| {
                let client = client.take();

                async move {
                    match client {
                        None => Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "Client already taken",
                        )),
                        Some(client) => Ok(client),
                    }
                }
            }))
            .await
            .unwrap();

        let grpc_reconciler = GrpcReconcile::new(channel, k8s_client);

        tokio::spawn(async move {
            let (request, send) = k8s_handle.next_request().await.unwrap();
            assert_eq!(request.method(), Method::GET);
            assert_eq!(request.uri().to_string(), "/api/v1/nodes?");

            let node = Node {
                metadata: Default::default(),
                spec: None,
                status: Some(NodeStatus {
                    addresses: Some(vec![NodeAddress {
                        type_: "InternalIP".to_string(),
                        address: "127.0.0.1".to_string(),
                    }]),
                    ..Default::default()
                }),
            };

            send.send_response(Response::new(hyper::Body::from(
                serde_json::to_string(&ObjectList {
                    metadata: Default::default(),
                    items: vec![node],
                })
                .unwrap(),
            )));

            let (mut request, send) = k8s_handle.next_request().await.unwrap();
            assert_eq!(request.method(), Method::PATCH);
            assert_eq!(
                request.uri().to_string(),
                "/api/v1/namespaces/default/services/test?"
            );

            let mut data = aggregate(request.body_mut()).await.unwrap();
            let patch: Value =
                serde_json::from_slice(&data.copy_to_bytes(data.remaining())).unwrap();

            let status = patch.get("status").unwrap();
            let status: ServiceStatus = serde_json::from_value(status.clone()).unwrap();
            assert_eq!(
                status,
                ServiceStatus {
                    conditions: None,
                    load_balancer: Some(LoadBalancerStatus {
                        ingress: Some(vec![LoadBalancerIngress {
                            hostname: None,
                            ip: Some("192.168.1.2".to_string()),
                            ports: None,
                        }])
                    }),
                }
            );

            let annotations = patch
                .get("metadata")
                .unwrap()
                .get("annotations")
                .unwrap()
                .as_object()
                .unwrap();
            let last_port_config = annotations
                .get(VMLB_ANNOTATION_PORTS_KEY)
                .unwrap()
                .as_str()
                .unwrap();
            let last_port_config: Vec<ServicePort> =
                serde_json::from_str(last_port_config).unwrap();
            assert_eq!(
                last_port_config.as_slice(),
                &[ServicePort {
                    app_protocol: None,
                    name: None,
                    node_port: Some(36666),
                    port: 8800,
                    protocol: Some("TCP".to_string()),
                    target_port: Some(IntOrString::Int(80)),
                }]
            );

            let finalizer = &patch
                .get("metadata")
                .unwrap()
                .get("finalizers")
                .unwrap()
                .as_array()
                .unwrap()[0];
            assert_eq!(finalizer.as_str().unwrap(), VMLB_FINALIZER);

            let service = Service {
                metadata: ObjectMeta {
                    namespace: Some("default".to_string()),
                    name: Some("test".to_string()),
                    finalizers: Some(vec![VMLB_FINALIZER.to_string()]),
                    ..Default::default()
                },
                spec: Some(ServiceSpec {
                    type_: Some("LoadBalancer".to_string()),
                    ports: Some(vec![ServicePort {
                        port: 80,
                        protocol: Some("TCP".to_string()),
                        target_port: Some(IntOrString::Int(80)),
                        node_port: Some(36666),
                        ..Default::default()
                    }]),
                    ..Default::default()
                }),
                status: Some(ServiceStatus {
                    load_balancer: Some(LoadBalancerStatus {
                        ingress: Some(vec![LoadBalancerIngress {
                            hostname: None,
                            ip: Some("192.168.1.1".to_string()),
                            ports: None,
                        }]),
                    }),
                    ..Default::default()
                }),
            };

            send.send_response(Response::new(hyper::Body::from(
                serde_json::to_string(&service).unwrap(),
            )));
        });

        let service = Arc::new(Service {
            metadata: ObjectMeta {
                namespace: Some("default".to_string()),
                name: Some("test".to_string()),
                annotations: Some(BTreeMap::from([(
                    VMLB_ANNOTATION_PORTS_KEY.to_string(),
                    serde_json::to_string(&[ServicePort {
                        node_port: Some(36666),
                        port: 80,
                        protocol: Some("TCP".to_string()),
                        target_port: Some(IntOrString::Int(80)),
                        ..Default::default()
                    }])
                    .unwrap(),
                )])),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                type_: Some("LoadBalancer".to_string()),
                ports: Some(vec![ServicePort {
                    port: 8800,
                    protocol: Some("TCP".to_string()),
                    target_port: Some(IntOrString::Int(80)),
                    node_port: Some(36666),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            status: Some(ServiceStatus {
                load_balancer: Some(LoadBalancerStatus {
                    ingress: Some(vec![LoadBalancerIngress {
                        ip: Some("192.168.1.1".to_string()),
                        ..Default::default()
                    }]),
                }),
                ..Default::default()
            }),
        });

        dbg!(grpc_reconciler.reconcile(service).await.unwrap());
    }

    #[tokio::test]
    async fn delete_service() {
        let (k8s_service, mut k8s_handle) = mock::pair::<_, Response<hyper::Body>>();

        struct TestAgentServer;

        #[async_trait]
        impl Agent for TestAgentServer {
            type RefreshServicesStream = stream::Empty<Result<AddServiceResponse, Status>>;

            async fn refresh_services(
                &self,
                _request: tonic::Request<Streaming<AddServiceRequest>>,
            ) -> Result<tonic::Response<Self::RefreshServicesStream>, Status> {
                unreachable!()
            }

            async fn add_service(
                &self,
                _request: tonic::Request<AddServiceRequest>,
            ) -> Result<tonic::Response<AddServiceResponse>, Status> {
                unreachable!()
            }

            async fn delete_service(
                &self,
                request: tonic::Request<DeleteServiceRequest>,
            ) -> Result<tonic::Response<DeleteServiceResponse>, Status> {
                assert_eq!(
                    request.into_inner(),
                    DeleteServiceRequest {
                        namespace: "default".to_string(),
                        service: "test".to_string(),
                    }
                );

                Ok(tonic::Response::new(DeleteServiceResponse {}))
            }
        }

        let (client, server) = tokio::io::duplex(4096);
        let mut client = Some(client);

        tokio::spawn(async move {
            Server::builder()
                .add_service(AgentServer::new(TestAgentServer))
                .serve_with_incoming(stream::iter(vec![Ok::<_, std::io::Error>(server)]))
                .await
        });

        let k8s_client = Client::new(k8s_service, "default");

        let channel = Endpoint::try_from("http://127.0.0.1:80")
            .unwrap()
            .connect_with_connector(tower::service_fn(move |_: Uri| {
                let client = client.take();

                async move {
                    match client {
                        None => Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "Client already taken",
                        )),
                        Some(client) => Ok(client),
                    }
                }
            }))
            .await
            .unwrap();

        let grpc_reconciler = GrpcReconcile::new(channel, k8s_client);

        tokio::spawn(async move {
            let (mut request, send) = k8s_handle.next_request().await.unwrap();
            assert_eq!(request.method(), Method::PATCH);
            assert_eq!(
                request.uri().to_string(),
                "/api/v1/namespaces/default/services/test?"
            );

            let mut data = aggregate(request.body_mut()).await.unwrap();
            let patch: Value =
                serde_json::from_slice(&data.copy_to_bytes(data.remaining())).unwrap();

            let status = patch.get("status").unwrap();
            let status: ServiceStatus = serde_json::from_value(status.clone()).unwrap();
            assert_eq!(
                status,
                ServiceStatus {
                    conditions: None,
                    load_balancer: None,
                }
            );

            let annotations = patch
                .get("metadata")
                .unwrap()
                .get("annotations")
                .unwrap()
                .as_object()
                .unwrap();
            assert!(annotations.get(VMLB_ANNOTATION_PORTS_KEY).is_none());

            if let Some(finalizers) = patch.get("metadata").unwrap().get("finalizers") {
                assert_eq!(
                    finalizers
                        .as_array()
                        .map(|finalizers| finalizers.len())
                        .unwrap_or_default(),
                    0
                );
            }

            let service = Service {
                metadata: ObjectMeta {
                    namespace: Some("default".to_string()),
                    name: Some("test".to_string()),
                    finalizers: Some(vec![VMLB_FINALIZER.to_string()]),
                    ..Default::default()
                },
                spec: Some(ServiceSpec {
                    type_: Some("LoadBalancer".to_string()),
                    ports: Some(vec![ServicePort {
                        port: 80,
                        protocol: Some("TCP".to_string()),
                        target_port: Some(IntOrString::Int(80)),
                        node_port: Some(36666),
                        ..Default::default()
                    }]),
                    ..Default::default()
                }),
                status: Some(ServiceStatus {
                    load_balancer: Some(LoadBalancerStatus {
                        ingress: Some(vec![LoadBalancerIngress {
                            hostname: None,
                            ip: Some("192.168.1.1".to_string()),
                            ports: None,
                        }]),
                    }),
                    ..Default::default()
                }),
            };

            send.send_response(Response::new(hyper::Body::from(
                serde_json::to_string(&service).unwrap(),
            )));
        });

        let service = Arc::new(Service {
            metadata: ObjectMeta {
                namespace: Some("default".to_string()),
                name: Some("test".to_string()),
                annotations: Some(BTreeMap::from([(
                    VMLB_ANNOTATION_PORTS_KEY.to_string(),
                    serde_json::to_string(&[ServicePort {
                        node_port: Some(36666),
                        port: 80,
                        protocol: Some("TCP".to_string()),
                        target_port: Some(IntOrString::Int(80)),
                        ..Default::default()
                    }])
                    .unwrap(),
                )])),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                type_: Some("LoadBalancer".to_string()),
                ports: Some(vec![ServicePort {
                    port: 8800,
                    protocol: Some("TCP".to_string()),
                    target_port: Some(IntOrString::Int(80)),
                    node_port: Some(36666),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            status: Some(ServiceStatus {
                load_balancer: Some(LoadBalancerStatus {
                    ingress: Some(vec![LoadBalancerIngress {
                        ip: Some("192.168.1.1".to_string()),
                        ..Default::default()
                    }]),
                }),
                ..Default::default()
            }),
        });

        dbg!(grpc_reconciler.delete(service).await.unwrap());
    }
}
