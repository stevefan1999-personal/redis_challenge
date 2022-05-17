use tokio::net::TcpListener;
use tokio::stream::StreamExt;

#[tokio::main]
async fn main() -> Result<(), &'static str> {
    let mut listener = TcpListener::bind("localhost:6379").await.map_err(|_| "failed to bind on 6379")?;
    while let Some(stream) = listener.incoming().filter_map(|x| x.ok()).next().await {
        println!("user connected: {}", stream.peer_addr().map_err(|_| "cannot get peer address")?);
        tokio::spawn(async {

        });
    }
    Ok(())
}
