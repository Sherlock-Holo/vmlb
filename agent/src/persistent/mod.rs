use std::collections::HashMap;
use std::error;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use crate::Network;

pub mod fs;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct Record {
    pub forwards: Vec<Forward>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct Forward {
    pub endpoints: Vec<String>,
    pub port: u16,
    pub protocol: Network,
    pub backends: Vec<SocketAddr>,
}

#[cfg_attr(test, mockall::automock(type Error = std::io::Error;))]
#[async_trait::async_trait]
pub trait Persistent {
    type Error: error::Error;

    async fn load_record(
        &self,
        namespace: &str,
        service: &str,
    ) -> Result<Option<Record>, Self::Error>;

    async fn load_all_records(&self) -> Result<HashMap<(String, String), Record>, Self::Error>;

    async fn store_record(
        &self,
        namespace: &str,
        service: &str,
        record: Record,
    ) -> Result<(), Self::Error>;

    async fn delete_record(&self, namespace: &str, service: &str) -> Result<(), Self::Error>;
}
