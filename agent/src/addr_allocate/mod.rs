use std::error;

pub use veth::VethAddrAllocate;

mod veth;

#[cfg_attr(test, mockall::automock(type Error = std::io::Error;))]
#[async_trait::async_trait]
pub trait AddrAllocate {
    type Error: error::Error;

    async fn allocate(&self, namespace: &str, service: &str) -> Result<Vec<String>, Self::Error>;

    async fn deallocate(&self, namespace: &str, service: &str) -> Result<(), Self::Error>;
}
