use std::io::{Error, ErrorKind, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use dashmap::DashMap;
use futures_util::future;
use futures_util::future::AbortHandle;
use rand::prelude::SliceRandom;
use tap::TapFallible;
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::{io, time};
use tracing::{error, info, warn};

use super::{Network, Proxy};

type UdpConnectionTrack = Arc<DashMap<(SocketAddr, SocketAddr), (Arc<UdpSocket>, Instant)>>;

#[derive(Debug, Hash, Eq, PartialEq)]
struct ProxyKey {
    listen_addr: SocketAddr,
    backends: Vec<SocketAddr>,
    network: Network,
}

#[derive(Debug)]
pub struct UserspaceProxy {
    proxies: DashMap<ProxyKey, AbortHandle>,
    udp_connection_track: UdpConnectionTrack,
    udp_connection_track_timeout: Duration,
}

impl UserspaceProxy {
    pub fn new(
        udp_connection_track_timeout: Duration,
        udp_connection_track_check_interval: Duration,
    ) -> Self {
        let proxy = Self {
            proxies: Default::default(),
            udp_connection_track: Arc::new(Default::default()),
            udp_connection_track_timeout,
        };

        proxy.start_udp_connection_track_check_task(udp_connection_track_check_interval);

        proxy
    }

    async fn start_tcp_proxy(
        &self,
        listen_addr: SocketAddr,
        backends: Vec<SocketAddr>,
    ) -> Result<()> {
        let listener = TcpListener::bind(listen_addr)
            .await
            .tap_err(|err| error!(%err, %listen_addr, "listen tcp failed"))?;

        let backends_key = backends.clone();

        let (fut, abort_handle) = future::abortable(async move {
            loop {
                let tcp_stream = match listener.accept().await {
                    Err(err) => {
                        warn!(%err, %listen_addr, "accept tcp failed");

                        continue;
                    }

                    Ok((tcp_stream, addr)) => {
                        info!(%addr, "accept tcp from peer");

                        tcp_stream
                    }
                };

                let backend = backends.choose(&mut rand::thread_rng()).copied().unwrap();

                info!(%backend, "choose backend");

                tokio::spawn(async move {
                    let backend_tcp = TcpStream::connect(backend)
                        .await
                        .tap_err(|err| error!(%err, %backend, "connect to backend failed"))?;

                    let (mut client_read, mut client_write) = tcp_stream.into_split();
                    let (mut backend_read, mut backend_write) = backend_tcp.into_split();

                    tokio::spawn(
                        async move { io::copy(&mut client_read, &mut backend_write).await },
                    );

                    tokio::spawn(
                        async move { io::copy(&mut backend_read, &mut client_write).await },
                    );

                    Ok::<_, Error>(())
                });
            }
        });

        tokio::spawn(fut);

        self.proxies.insert(
            ProxyKey {
                listen_addr,
                backends: backends_key,
                network: Network::TCP,
            },
            abort_handle,
        );

        Ok(())
    }

    async fn start_udp_proxy(
        &self,
        listen_addr: SocketAddr,
        backends: Vec<SocketAddr>,
    ) -> Result<()> {
        let listener = UdpSocket::bind(listen_addr)
            .await
            .tap_err(|err| error!(%err, %listen_addr, "listen tcp failed"))?;
        let listener = Arc::new(listener);

        let backends_key = backends.clone();
        let udp_connection_track = self.udp_connection_track.clone();

        let (fut, abort_handle) = future::abortable(async move {
            let mut buf = BytesMut::zeroed(8192);

            loop {
                let (data, addr) = match listener.recv_from(&mut buf).await {
                    Err(err) => {
                        warn!(%listen_addr, %err, "recv udp packet failed");

                        continue;
                    }

                    Ok((n, addr)) => (&buf[..n], addr),
                };

                match udp_connection_track.get_mut(&(addr, listen_addr)) {
                    None => {
                        let udp_socket = match UdpSocket::bind("0.0.0.0:0").await {
                            Err(err) => {
                                error!(%err, "bind udp socket failed");

                                continue;
                            }

                            Ok(udp_socket) => udp_socket,
                        };

                        let backend = backends.choose(&mut rand::thread_rng()).copied().unwrap();

                        if let Err(err) = udp_socket.connect(backend).await {
                            error!(%err, %backend, "connect udp socket failed");

                            continue;
                        }

                        if let Err(err) = udp_socket.send(data).await {
                            error!(%err, %backend, "send udp socket data failed");

                            continue;
                        }

                        let udp_socket = Arc::new(udp_socket);
                        {
                            let listener = listener.clone();
                            let udp_socket = Arc::downgrade(&udp_socket);

                            tokio::spawn(async move {
                                let timeout = Duration::from_secs(5);
                                let mut buf = BytesMut::zeroed(8192);

                                loop {
                                    let udp_socket = match udp_socket.upgrade() {
                                        None => return Ok::<_, Error>(()),
                                        Some(udp_socket) => udp_socket,
                                    };

                                    let n = match time::timeout(timeout, async {
                                        udp_socket.recv(&mut buf).await
                                    }).await {
                                        Err(_) => continue,
                                        Ok(result) => {
                                            result.tap_err(|err| error!(%err, %addr, "recv udp socket data failed"))?
                                        }
                                    };

                                    listener.send_to(&buf[..n], addr).await.tap_err(
                                        |err| error!(%err, %addr, "send udp socket data failed"),
                                    )?;
                                }
                            });
                        }

                        udp_connection_track
                            .insert((addr, listen_addr), (udp_socket, Instant::now()));
                    }

                    Some(mut value) => {
                        let (udp_socket, instant) = &mut *value;

                        if let Err(err) = udp_socket.send(data).await {
                            error!(%err, %addr, "send udp socket data failed");

                            drop(value);

                            udp_connection_track.remove(&(addr, listen_addr));

                            continue;
                        }

                        *instant = Instant::now();
                    }
                }
            }
        });

        tokio::spawn(fut);

        self.proxies.insert(
            ProxyKey {
                listen_addr,
                backends: backends_key,
                network: Network::UDP,
            },
            abort_handle,
        );

        Ok(())
    }

    fn start_udp_connection_track_check_task(&self, check_interval: Duration) {
        let udp_connection_track = self.udp_connection_track.clone();
        let timeout = self.udp_connection_track_timeout;

        tokio::spawn(async move {
            loop {
                time::sleep(check_interval).await;

                let timeout_keys = udp_connection_track
                    .iter()
                    .filter_map(|ref_mut| {
                        if ref_mut.value().1.elapsed() >= timeout {
                            Some(*ref_mut.key())
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();

                for key in timeout_keys {
                    // double check
                    udp_connection_track
                        .remove_if(&key, |_, (_, instant)| instant.elapsed() >= timeout);
                }
            }
        });
    }
}

#[async_trait::async_trait]
impl Proxy for UserspaceProxy {
    async fn start_proxy(
        &self,
        listen_addrs: &[String],
        backends: &[SocketAddr],
        network: Network,
    ) -> Result<()> {
        if listen_addrs.is_empty() {
            error!("empty listen addr");

            return Err(Error::new(ErrorKind::Other, "empty listen addr"));
        }

        if backends.is_empty() {
            error!("empty backends");

            return Err(Error::new(ErrorKind::Other, "empty backends"));
        }

        let listen_addr = listen_addrs
            .iter()
            .map(|addr| {
                addr.parse().map_err(|err| {
                    error!(%err, %addr, "invalid addr");

                    Error::new(ErrorKind::Other, err)
                })
            })
            .collect::<Result<Vec<_>>>()?;

        info!(?listen_addr, ?network, "parse listen addr done");

        match network {
            Network::TCP => {
                for addr in listen_addr {
                    self.start_tcp_proxy(addr, backends.to_vec()).await?;
                }
            }
            Network::UDP => {
                for addr in listen_addr {
                    self.start_udp_proxy(addr, backends.to_vec()).await?;
                }
            }
        }

        Ok(())
    }

    async fn stop_proxy(
        &self,
        listen_addrs: &[String],
        backends: &[SocketAddr],
        network: Network,
    ) -> Result<()> {
        if listen_addrs.is_empty() {
            error!("empty listen addr");

            return Err(Error::new(ErrorKind::Other, "empty listen addr"));
        }

        let listen_addr = listen_addrs
            .iter()
            .map(|addr| {
                addr.parse().map_err(|err| {
                    error!(%err, %addr, "invalid addr");

                    Error::new(ErrorKind::Other, err)
                })
            })
            .collect::<Result<Vec<_>>>()?;

        for listen_addr in listen_addr {
            if let Some((_, abort_handle)) = self.proxies.remove(&ProxyKey {
                listen_addr,
                backends: backends.to_vec(),
                network,
            }) {
                abort_handle.abort();
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use rand::Rng;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    #[tokio::test]
    async fn test_new() {
        let proxy = UserspaceProxy::new(Duration::from_secs(5 * 60), Duration::from_secs(5));

        assert_eq!(
            proxy.udp_connection_track_timeout,
            Duration::from_secs(5 * 60)
        );
    }

    #[tokio::test]
    async fn test_tcp_proxy() {
        let proxy = UserspaceProxy::new(Duration::from_secs(5 * 60), Duration::from_secs(5));

        let backend = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let backend_addr = backend.local_addr().unwrap();

        let port = rand::thread_rng().gen_range(60000..=65535);
        let listen_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::from([127, 0, 0, 1])), port);

        proxy
            .start_tcp_proxy(listen_addr, vec![backend_addr])
            .await
            .unwrap();

        assert_eq!(
            TcpListener::bind(listen_addr).await.unwrap_err().kind(),
            ErrorKind::AddrInUse
        );

        tokio::spawn(async move {
            let mut tcp_stream = TcpStream::connect(listen_addr).await.unwrap();

            tcp_stream.write_all(b"test").await.unwrap();
        });

        let (mut tcp_stream, _) = backend.accept().await.unwrap();

        let mut buf = [0; 4];
        tcp_stream.read_exact(&mut buf).await.unwrap();

        assert_eq!(&buf, b"test");
    }

    #[tokio::test]
    async fn test_udp_proxy() {
        let proxy = UserspaceProxy::new(Duration::from_secs(5 * 60), Duration::from_secs(5));

        let backend = UdpSocket::bind("0.0.0.0:0").await.unwrap();
        let backend_addr = backend.local_addr().unwrap();

        let port = rand::thread_rng().gen_range(60000..=65535);
        let listen_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::from([127, 0, 0, 1])), port);

        proxy
            .start_proxy(&[listen_addr.to_string()], &[backend_addr], Network::UDP)
            .await
            .unwrap();

        assert_eq!(
            UdpSocket::bind(listen_addr).await.unwrap_err().kind(),
            ErrorKind::AddrInUse
        );

        tokio::spawn(async move {
            let udp_socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();
            udp_socket.connect(listen_addr).await.unwrap();

            udp_socket.send(b"test").await.unwrap();
        });

        let mut buf = [0; 4];
        let (_, addr) = backend.recv_from(&mut buf).await.unwrap();

        dbg!(addr);
        assert_eq!(&buf, b"test");
    }

    #[tokio::test]
    async fn stop_tcp_proxy() {
        let proxy = UserspaceProxy::new(Duration::from_secs(5 * 60), Duration::from_secs(5));

        let backend = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let backend_addr = backend.local_addr().unwrap();

        let port = rand::thread_rng().gen_range(60000..=65535);
        let listen_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::from([127, 0, 0, 1])), port);

        proxy
            .start_proxy(&[listen_addr.to_string()], &[backend_addr], Network::TCP)
            .await
            .unwrap();

        proxy
            .stop_proxy(&[listen_addr.to_string()], &[backend_addr], Network::TCP)
            .await
            .unwrap();

        time::sleep(Duration::from_millis(500)).await;

        TcpListener::bind(listen_addr).await.unwrap();
    }

    #[tokio::test]
    async fn stop_udp_proxy() {
        let proxy = UserspaceProxy::new(Duration::from_secs(5 * 60), Duration::from_secs(5));

        let backend = UdpSocket::bind("0.0.0.0:0").await.unwrap();
        let backend_addr = backend.local_addr().unwrap();

        let port = rand::thread_rng().gen_range(60000..=65535);
        let listen_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::from([127, 0, 0, 1])), port);

        proxy
            .start_proxy(&[listen_addr.to_string()], &[backend_addr], Network::UDP)
            .await
            .unwrap();

        proxy
            .stop_proxy(&[listen_addr.to_string()], &[backend_addr], Network::UDP)
            .await
            .unwrap();

        time::sleep(Duration::from_millis(300)).await;

        UdpSocket::bind(listen_addr).await.unwrap();
    }
}
