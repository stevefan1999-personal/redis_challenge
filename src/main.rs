use crate::server::RedisServer;

#[tokio::main]
async fn main() -> util::Result<()> {
    let mut server = RedisServer::new("127.0.0.1:6379").await?;
    server.serve().await
}

mod command;
mod data_type;
mod server;
mod util;
