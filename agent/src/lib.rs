use std::error::Error;

use crate::proxy::Network;

mod addr_allocate;
mod api;
mod persistent;
mod proxy;

pub async fn run() -> Result<(), Box<dyn Error>> {
    todo!()
}
