use std::io::{Error, ErrorKind};
use std::net::Ipv4Addr;
use std::os::unix::ffi::OsStrExt;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashSet;
use futures_util::future::AbortHandle;
use futures_util::{future, StreamExt, TryStreamExt};
use md5::{Digest, Md5};
use nix::errno::Errno;
use rtnetlink::packet::address::Nla as AddressNla;
use rtnetlink::packet::link::nlas::{Info, InfoKind, Nla as LinkNla};
use rtnetlink::packet::{AddressMessage, LinkMessage, AF_INET};
use rtnetlink::Handle;
use tap::TapFallible;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::{fs, time};
use tokio_stream::wrappers::IntervalStream;
use tracing::{error, info, warn};

use crate::addr_allocate::AddrAllocate;

const MAX_NIC_NAME_LEN: usize = 13;

pub struct VethAddrAllocate {
    inner: Arc<VethAddrAllocateInner>,
}

struct VethAddrAllocateInner {
    bridge_index: u32,
    handle: Handle,
    netlink_task_stop: AbortHandle,
    nic_list: DashSet<String>,
}

impl VethAddrAllocate {
    pub async fn new(bridge_name: &str) -> Result<Self, Error> {
        let (conn, handle, _) = rtnetlink::new_connection()?;
        let (conn, abort_handle) = future::abortable(conn);

        tokio::spawn(async move {
            let _ = conn.await;
        });

        let link_message = handle
            .link()
            .get()
            .match_name(bridge_name.to_string())
            .execute()
            .try_next()
            .await
            .map_err(|err| Error::new(ErrorKind::Other, err))?
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::NotFound,
                    format!("bridge {} not found", bridge_name),
                )
            })?;

        let inner = Arc::new(VethAddrAllocateInner {
            bridge_index: link_message.header.index,
            handle,
            netlink_task_stop: abort_handle,
            nic_list: Default::default(),
        });

        {
            // for now we don't want the VethAddrAllocate cloneable yet
            let inner = inner.clone();
            tokio::spawn(async move { Self { inner }.check_dhclient_alive().await });
        }

        Ok(Self { inner })
    }

    async fn allocate_veth(&self, nic_name: &str, peer: &str) -> Result<(), Error> {
        self.inner
            .handle
            .link()
            .add()
            .veth(nic_name.to_string(), peer.to_string())
            .execute()
            .await
            .map_err(|err| {
                error!(%err, nic_name, peer, "allocate veth failed");

                Error::new(ErrorKind::Other, err)
            })?;

        info!(nic_name, peer, "add veth done");

        let peer_info = self
            .inner
            .handle
            .link()
            .get()
            .match_name(peer.to_string())
            .execute()
            .try_next()
            .await
            .map_err(|err| {
                error!(%err, peer, "get peer nic info failed");

                Error::new(ErrorKind::Other, err)
            })?
            .ok_or_else(|| {
                error!(peer, "get peer nic info failed, peer not exists");

                Error::new(
                    ErrorKind::NotFound,
                    format!("get peer {} nic info failed, peer not exists", peer),
                )
            })?;

        info!(
            nic_name,
            peer,
            peer_index = peer_info.header.index,
            "get peer index done"
        );

        self.inner
            .handle
            .link()
            .set(peer_info.header.index)
            .master(self.inner.bridge_index)
            .execute()
            .await
            .map_err(|err| {
                error!(%err, "set peer master to bridge failed");

                Error::new(ErrorKind::Other, err)
            })?;

        info!(
            nic_name,
            peer,
            bridge_index = self.inner.bridge_index,
            "set peer master to bridge done"
        );

        Ok(())
    }

    async fn allocate_ip(&self, nic_name: &str) -> Result<Vec<String>, Error> {
        let pid_file = format!("/run/dhclient.{}.pid", nic_name);

        let mut child = Command::new("dhclient")
            .args(["-pf", &pid_file])
            .arg(nic_name)
            .spawn()
            .tap_err(|err| error!(%err, nic_name, "spawn dhclient failed"))?;
        let exit_status = child
            .wait()
            .await
            .tap_err(|err| error!(%err, nic_name, "wait dhclient process failed"))?;

        if !exit_status.success() {
            if let Some(exit_code) = exit_status.code() {
                error!(nic_name, exit_code, "dhclient failed");
            } else {
                error!(nic_name, "dhclient failed");
            }

            let mut stdout = child.stdout.take().unwrap();
            let mut stdout_buf = vec![];
            if stdout
                .read_to_end(&mut stdout_buf)
                .await
                .tap_err(|err| error!(%err, nic_name, "read dhclient stdout failed"))
                .is_ok()
            {
                error!(nic_name, stdout = ?String::from_utf8_lossy(&stdout_buf), "dhclient stdout");
            }

            let mut stderr = child.stderr.take().unwrap();
            let mut stderr_buf = vec![];
            if stderr
                .read_to_end(&mut stderr_buf)
                .await
                .tap_err(|err| error!(%err, nic_name, "read dhclient stderr failed"))
                .is_ok()
            {
                error!(nic_name, stderr = ?String::from_utf8_lossy(&stderr_buf), "dhclient stdout");
            }

            return Err(Error::new(
                ErrorKind::Other,
                format!("allocate ip for nic {} by dhclient failed", nic_name),
            ));
        }

        info!(nic_name, "run dhclient done");

        let link_message = self.get_nic_info(nic_name).await?.ok_or_else(|| {
            error!(nic_name, "get exists nic info failed");

            Error::new(ErrorKind::NotFound, format!("nic {} not exists", nic_name))
        })?;

        let veth_ip = self.get_exists_veth_ip(nic_name, &link_message).await?;

        info!(nic_name, ?veth_ip, "get veth ip done");

        self.inner.nic_list.insert(nic_name.to_string());

        Ok(veth_ip)
    }

    async fn nic_is_veth(&self, nic_name: &str, link_message: &LinkMessage) -> Result<bool, Error> {
        let info = match link_message.nlas.iter().find_map(|nla| {
            if let LinkNla::Info(info) = nla {
                Some(info)
            } else {
                None
            }
        }) {
            None => {
                error!(nic_name, "nic exists but contains no info");

                return Err(Error::new(
                    ErrorKind::Other,
                    "nic exists but contains no info",
                ));
            }

            Some(info) => info,
        };

        Ok(info.iter().any(|info| {
            if let Info::Kind(kind) = info {
                kind == &InfoKind::Veth
            } else {
                false
            }
        }))
    }

    async fn get_exists_veth_ip(
        &self,
        nic_name: &str,
        link_message: &LinkMessage,
    ) -> Result<Vec<String>, Error> {
        let addresses = self
            .inner
            .handle
            .address()
            .get()
            .set_link_index_filter(link_message.header.index)
            .execute()
            .try_collect::<Vec<AddressMessage>>()
            .await
            .map_err(|err| {
                error!(%err, nic_name, "get nic addresses failed");

                Error::new(ErrorKind::Other, err)
            })?;

        let addrs = addresses
            .into_iter()
            .filter(|address| address.header.family as u16 == AF_INET)
            .flat_map(|address| address.nlas)
            .filter_map(|nla| {
                if let AddressNla::Address(addr) = nla {
                    if addr.len() == 4 {
                        Some(Ipv4Addr::new(addr[0], addr[1], addr[2], addr[3]).to_string())
                    } else {
                        warn!(?addr, "ignore invalid addr");

                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        Ok(addrs)
    }

    async fn get_nic_info(&self, nic_name: &str) -> Result<Option<LinkMessage>, Error> {
        match self
            .inner
            .handle
            .link()
            .get()
            .match_name(nic_name.to_string())
            .execute()
            .try_next()
            .await
        {
            Err(rtnetlink::Error::NetlinkError(err))
                if err.to_io().raw_os_error() == Some(Errno::ENODEV as i32) =>
            {
                Ok(None)
            }

            Err(err) => {
                error!(%err, nic_name, "get nic info failed");

                Err(Error::new(ErrorKind::Other, err))
            }

            Ok(link) => Ok(link),
        }
    }

    async fn deallocate_ip(&self, nic_name: &str) -> Result<(), Error> {
        let mut child = Command::new("dhclient")
            .args(["-r", nic_name])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .tap_err(|err| error!(%err, nic_name, "spawn dhclient -r failed"))?;
        let exit_status = child
            .wait()
            .await
            .tap_err(|err| error!(%err, nic_name, "wait dhclient -r process failed"))?;

        if !exit_status.success() {
            if let Some(exit_code) = exit_status.code() {
                error!(exit_code, nic_name, "dhclient -r failed");
            } else {
                error!(nic_name, "dhclient -r failed");
            }

            let mut stdout = child.stdout.take().unwrap();
            let mut stdout_buf = vec![];
            if stdout
                .read_to_end(&mut stdout_buf)
                .await
                .tap_err(|err| error!(%err, nic_name, "read dhclient -r stdout failed"))
                .is_ok()
            {
                error!(nic_name, stdout = ?String::from_utf8_lossy(&stdout_buf), "dhclient -r stdout");
            }

            let mut stderr = child.stderr.take().unwrap();
            let mut stderr_buf = vec![];
            if stderr
                .read_to_end(&mut stderr_buf)
                .await
                .tap_err(|err| error!(%err, nic_name, "read dhclient -r stderr failed"))
                .is_ok()
            {
                error!(nic_name, stderr = ?String::from_utf8_lossy(&stderr_buf), "dhclient -r stdout");
            }
        }

        Ok(())
    }

    async fn check_dhclient_alive(&self) {
        let mut interval = IntervalStream::new(time::interval(Duration::from_secs(10)));

        while (interval.next().await).is_some() {
            let mut dead_dhcp_nic_list = vec![];

            for nic_name in self.inner.nic_list.iter() {
                let nic_name = &*nic_name;
                let pid_file = format!("/run/dhclient.{}.pid", nic_name);

                let pid = match fs::read(&pid_file).await {
                    Err(err) if err.kind() == ErrorKind::NotFound => {
                        warn!(%nic_name, %err, "nic dhclient instance miss");

                        dead_dhcp_nic_list.push(nic_name.clone());

                        continue;
                    }

                    Err(err) => {
                        error!(%nic_name, %err, "read nic dhclient pid file failed");

                        dead_dhcp_nic_list.push(nic_name.clone());

                        continue;
                    }

                    Ok(pid) => pid,
                };
                let pid = String::from_utf8_lossy(&pid);
                let pid = match pid.parse::<u32>() {
                    Err(err) => {
                        error!(%nic_name, %pid, %err, "parse pid to u32 failed");

                        dead_dhcp_nic_list.push(nic_name.clone());

                        continue;
                    }

                    Ok(pid) => pid,
                };

                info!(nic_name, pid, "get forked dhclient pid done");

                match fs::read_link(format!("/proc/{}/exe", pid)).await {
                    Err(err) if err.kind() == ErrorKind::NotFound => {
                        warn!(%nic_name, %err, "nic dhclient instance miss");

                        dead_dhcp_nic_list.push(nic_name.clone());
                    }

                    Err(err) => {
                        error!(%nic_name, pid, %err, "try to check dhclient instance failed");

                        dead_dhcp_nic_list.push(nic_name.clone());
                    }

                    Ok(path) => {
                        if !path
                            .as_os_str()
                            .as_bytes()
                            .windows("dhclient".chars().count())
                            .any(|sub_path| sub_path == b"dhclient")
                        {
                            error!(%nic_name, pid, "dhclient pid exists, but it is not dhclient instance");

                            dead_dhcp_nic_list.push(nic_name.clone());
                        }
                    }
                }
            }

            if !dead_dhcp_nic_list.is_empty() {
                info!(?dead_dhcp_nic_list, "start reallocate ip for dead dhcp nic");

                for nic_name in dead_dhcp_nic_list.iter() {
                    match self.allocate_ip(nic_name).await {
                        Err(err) => {
                            error!(%nic_name, %err, "reallocate ip for dead dhcp nic failed");
                        }

                        Ok(ip) => {
                            // dhclient will try to reuse allocated ip, so no need to pass the ip
                            // out
                            info!(%nic_name, ?ip, "reallocate ip for dead dhcp nic done");
                        }
                    }
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl AddrAllocate for VethAddrAllocate {
    type Error = Error;

    async fn allocate(&self, namespace: &str, service: &str) -> Result<Vec<String>, Self::Error> {
        let (nic_name, peer) = get_nic_names(namespace, service);

        let link_message = self
            .get_nic_info(&nic_name)
            .await
            .tap_ok(|link| if link.is_none() {})?;

        if let Some(link_message) = link_message {
            let is_veth = self.nic_is_veth(&nic_name, &link_message).await?;
            if !is_veth {
                error!(%nic_name, "nic exists but is not veth");

                return Err(Error::new(
                    ErrorKind::Other,
                    format!("nic {} exists but is not veth", nic_name),
                ));
            }

            let addrs = self.get_exists_veth_ip(&nic_name, &link_message).await?;
            if !addrs.is_empty() {
                info!(namespace, service, ?addrs, "reuse exists addrs");

                return Ok(addrs);
            }

            let addrs = self.allocate_ip(&nic_name).await?;

            info!(namespace, service, ?addrs, "allocate addrs done");

            return Ok(addrs);
        }

        self.allocate_veth(&nic_name, &peer).await?;

        info!(%nic_name, "allocate veth done");

        let addrs = self.allocate_ip(&nic_name).await?;

        info!(namespace, service, ?addrs, "allocate addrs done");

        Ok(addrs)
    }

    async fn deallocate(&self, namespace: &str, service: &str) -> Result<(), Self::Error> {
        let (nic_name, _) = get_nic_names(namespace, service);

        self.deallocate_ip(&nic_name).await?;

        info!(%nic_name, "deallocate ip on nic done");

        let link_message = match self.get_nic_info(&nic_name).await? {
            None => {
                info!(%nic_name, "nic is not exists");

                return Ok(());
            }

            Some(link_message) => link_message,
        };

        self.inner
            .handle
            .link()
            .del(link_message.header.index)
            .execute()
            .await
            .map_err(|err| {
                error!(%err, %nic_name, "delete nic failed");

                Error::new(ErrorKind::Other, err)
            })
    }
}

impl Drop for VethAddrAllocate {
    fn drop(&mut self) {
        self.inner.netlink_task_stop.abort();
    }
}

fn get_nic_names(namespace: &str, name: &str) -> (String, String) {
    let mut buf = [0u8; 16].into();
    let mut hasher = Md5::new();

    hasher.update(format!("{}-{}", namespace, name).as_bytes());
    hasher.finalize_into_reset(&mut buf);

    let mut nic_name = format!("{:x}", buf);
    nic_name.truncate(MAX_NIC_NAME_LEN);

    hasher.update(format!("{}-{}-peer", namespace, name).as_bytes());
    hasher.finalize_into(&mut buf);

    let mut peer = format!("{:x}", buf);
    peer.truncate(MAX_NIC_NAME_LEN);

    (nic_name, peer)
}
