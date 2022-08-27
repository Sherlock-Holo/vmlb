use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::path::PathBuf;

use futures_util::stream::FuturesUnordered;
use futures_util::{future, TryStreamExt};
use tap::TapFallible;
use tokio::fs;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio_stream::wrappers::ReadDirStream;
use tracing::{error, info};

use super::{Persistent, Record};

#[derive(Debug)]
pub struct FsPersistent {
    dir: PathBuf,
}

impl FsPersistent {
    pub async fn new(dir: PathBuf) -> Result<Self, Error> {
        fs::create_dir_all(&dir).await?;

        Ok(Self { dir })
    }
}

#[async_trait::async_trait]
impl Persistent for FsPersistent {
    type Error = Error;

    async fn load_record(
        &self,
        namespace: &str,
        service: &str,
    ) -> Result<Option<Record>, Self::Error> {
        let mut path = self.dir.clone();
        path.push(namespace);
        path.push(service);

        let content = match fs::read(&path).await {
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                error!(%err, ?path, "load record failed");

                return Err(err);
            }

            Ok(content) => content,
        };

        info!(namespace, service, "read record content done");

        match serde_json::from_slice(&content) {
            Err(err) => {
                error!(%err, "unmarshal record failed");

                Err(Error::new(ErrorKind::Other, err))
            }

            Ok(record) => Ok(Some(record)),
        }
    }

    async fn load_all_records(&self) -> Result<HashMap<(String, String), Record>, Self::Error> {
        let dir = fs::read_dir(&self.dir)
            .await
            .tap_err(|err| error!(%err, dir = ?self.dir, "open record root dir failed"))?;
        let mut dir = ReadDirStream::new(dir);
        let futs = FuturesUnordered::new();

        while let Some(ns_entry) = dir
            .try_next()
            .await
            .tap_err(|err| error!(%err, dir = ?self.dir, "read record root dir failed"))?
        {
            let path = ns_entry.path();
            let namespace = path.file_name().ok_or_else(|| {
                error!(?path, "path has no filename");

                Error::new(ErrorKind::Other, format!("invalid path {:?}", path))
            })?;

            let metadata = ns_entry
                .metadata()
                .await
                .tap_err(|err| error!(%err, ?path, "get namespace entry metadata failed"))?;

            if !metadata.is_dir() {
                continue;
            }

            let ns_dir = fs::read_dir(&path)
                .await
                .tap_err(|err| error!(%err, ?path, "open namespace dir failed"))?;
            let ns_dir = ReadDirStream::new(ns_dir);

            ns_dir
                .and_then(|entry| {
                    let path = entry.path();
                    let service = path.file_name().ok_or_else(|| {
                        error!(?path, "path has no filename");

                        Error::new(ErrorKind::Other, format!("invalid path {:?}", path))
                    });
                    let service = service
                        .map(|svc| svc.to_string_lossy().to_string())
                        .map(|svc| (path, svc));

                    future::ready(service)
                })
                .try_for_each(|(path, service)| async {
                    let namespace = namespace.to_string_lossy().to_string();

                    futs.push(async move {
                        let content = fs::read(&path)
                            .await
                            .tap_err(|err| error!(%err, ?path, "read reocrd content failed"))?;

                        match serde_json::from_slice::<Record>(&content) {
                            Err(err) => {
                                error!(%err, ?path, "unmarshal record failed");

                                Err(Error::new(ErrorKind::Other, err))
                            }

                            Ok(record) => Ok(((namespace, service), record)),
                        }
                    });

                    Ok(())
                })
                .await
                .tap_err(|err| error!(%err, ?path, "read namespace dir failed"))?;
        }

        futs.try_collect::<HashMap<_, _>>()
            .await
            .tap_err(|err| error!(%err, "collect records failed"))
    }

    async fn store_record(
        &self,
        namespace: &str,
        service: &str,
        record: Record,
    ) -> Result<(), Self::Error> {
        let content = match serde_json::to_string(&record) {
            Err(err) => {
                error!(%err, "marshal record failed");

                return Err(Error::new(ErrorKind::Other, err));
            }

            Ok(content) => content,
        };

        let mut path = self.dir.join(namespace);

        fs::create_dir_all(&path)
            .await
            .tap_err(|err| error!(%err, namespace, service, "create namespace dir failed"))?;

        path.push(service);

        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(path)
            .await
            .tap_err(|err| error!(%err, namespace, service, "create service record file failed"))?;

        info!(namespace, service, "create service record file done");

        file.write_all(content.as_bytes()).await.tap_err(
            |err| error!(%err, namespace, service, "write service record content failed"),
        )?;

        info!(namespace, service, "write service record content done");

        Ok(())
    }

    async fn delete_record(&self, namespace: &str, service: &str) -> Result<(), Self::Error> {
        let mut path = self.dir.join(namespace);
        path.push(service);

        match fs::remove_file(&path).await {
            Err(err) if err.kind() == ErrorKind::NotFound => {
                info!(namespace, service, "record is not exists");

                Ok(())
            }

            Err(err) => {
                error!(namespace, service, "delete record failed");

                Err(err)
            }

            Ok(_) => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::str::FromStr;

    use tempfile::TempDir;

    use super::*;
    use crate::persistent::Forward;
    use crate::Network;

    #[tokio::test]
    async fn store_record() {
        let temp_dir = TempDir::new().unwrap();
        let persistent = FsPersistent::new(temp_dir.path().to_path_buf())
            .await
            .unwrap();

        let record = Record {
            forwards: vec![Forward {
                endpoints: vec!["1.1.1.1".to_string()],
                port: 80,
                protocol: Network::TCP,
                backends: vec![SocketAddr::from_str("127.0.0.1:80").unwrap()],
            }],
        };

        persistent
            .store_record("default", "test", record)
            .await
            .unwrap();

        let content = fs::read(temp_dir.path().join("default").join("test"))
            .await
            .unwrap();

        assert_eq!(
            String::from_utf8(content).unwrap(),
            r#"{"forwards":[{"endpoints":["1.1.1.1"],"port":80,"protocol":"TCP","backends":["127.0.0.1:80"]}]}"#,
        );
    }

    #[tokio::test]
    async fn load_record() {
        let temp_dir = TempDir::new().unwrap();
        let persistent = FsPersistent::new(temp_dir.path().to_path_buf())
            .await
            .unwrap();

        let mut path = temp_dir.path().join("default");

        fs::create_dir(&path).await.unwrap();

        path.push("test");

        fs::write(
            path,
            r#"{"forwards":[{"endpoints":["1.1.1.1"],"port":80,"protocol":"TCP","backends":["127.0.0.1:80"]}]}"#,
        )
            .await
            .unwrap();

        let record = persistent
            .load_record("default", "test")
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            record,
            Record {
                forwards: vec![Forward {
                    endpoints: vec!["1.1.1.1".to_string()],
                    port: 80,
                    protocol: Network::TCP,
                    backends: vec![SocketAddr::from_str("127.0.0.1:80").unwrap()],
                }]
            }
        );
    }

    #[tokio::test]
    async fn load_all_records() {
        let temp_dir = TempDir::new().unwrap();
        let persistent = FsPersistent::new(temp_dir.path().to_path_buf())
            .await
            .unwrap();

        let mut path = temp_dir.path().join("default");

        fs::create_dir(&path).await.unwrap();

        path.push("test");

        fs::write(
            path,
            r#"{"forwards":[{"endpoints":["1.1.1.1"],"port":80,"protocol":"TCP","backends":["127.0.0.1:80"]}]}"#,
        )
            .await
            .unwrap();

        let records = persistent.load_all_records().await.unwrap();
        let expect = HashMap::from([(
            ("default".to_string(), "test".to_string()),
            Record {
                forwards: vec![Forward {
                    endpoints: vec!["1.1.1.1".to_string()],
                    port: 80,
                    protocol: Network::TCP,
                    backends: vec![SocketAddr::from_str("127.0.0.1:80").unwrap()],
                }],
            },
        )]);

        assert_eq!(records, expect);
    }

    #[tokio::test]
    async fn load_record_namespace_not_found() {
        let temp_dir = TempDir::new().unwrap();
        let persistent = FsPersistent::new(temp_dir.path().to_path_buf())
            .await
            .unwrap();

        assert!(persistent
            .load_record("default", "test")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn load_record_service_not_found() {
        let temp_dir = TempDir::new().unwrap();
        let persistent = FsPersistent::new(temp_dir.path().to_path_buf())
            .await
            .unwrap();

        fs::create_dir(temp_dir.path().join("default"))
            .await
            .unwrap();

        assert!(persistent
            .load_record("default", "test")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn delete_record() {
        let temp_dir = TempDir::new().unwrap();
        let persistent = FsPersistent::new(temp_dir.path().to_path_buf())
            .await
            .unwrap();

        fs::create_dir(temp_dir.path().join("default"))
            .await
            .unwrap();
        fs::write(temp_dir.path().join("default").join("test"), b"test")
            .await
            .unwrap();

        persistent.delete_record("default", "test").await.unwrap();
    }

    #[tokio::test]
    async fn delete_record_not_found() {
        let temp_dir = TempDir::new().unwrap();
        let persistent = FsPersistent::new(temp_dir.path().to_path_buf())
            .await
            .unwrap();

        fs::create_dir(temp_dir.path().join("default"))
            .await
            .unwrap();

        persistent.delete_record("default", "test").await.unwrap();
        persistent.delete_record("not-found", "test").await.unwrap();
    }
}
