use crate::server::RedisServer;
use std::sync::Arc;

#[tokio::main]
async fn main() -> util::Result<()> {
    let server = Arc::new(RedisServer::new());
    server.serve("127.0.0.1:6379").await
}

mod command;
mod data_type;
mod server;
mod util;
