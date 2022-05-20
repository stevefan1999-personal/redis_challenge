use crate::{
    command::{Command, RespCommand},
    data_type::{RedisDataType, RespDataType},
};
use std::{collections::HashMap, convert::TryInto, sync::Arc};
use tokio::{
    io::{AsyncWriteExt, BufStream},
    net::{TcpListener, TcpStream, ToSocketAddrs},
    stream::StreamExt,
    sync::Mutex,
};

pub struct RedisServer {
    db: Arc<Mutex<HashMap<String, RedisDataType>>>,
}

impl RedisServer {
    pub fn new() -> RedisServer {
        RedisServer {
            db: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn process(&self, stream: TcpStream) -> crate::util::Result<()> {
        let mut stream = BufStream::new(stream);
        loop {
            let ret = match RespDataType::deserialize(&mut stream)
                .await
                .and_then(|data| data.try_into() as Result<RespCommand, _>)
            {
                Ok(cmd) => {
                    dbg!(&cmd);
                    match cmd {
                        RespCommand::Ping(ping) => ping.execute(&mut ()).await,
                        RespCommand::Echo(echo) => echo.execute(&mut ()).await,
                        RespCommand::Set(set) => {
                            let db = self.db.clone();
                            let mut db = db.lock().await;
                            set.execute(&mut db).await
                        }
                        RespCommand::Get(get) => {
                            let db = self.db.clone();
                            let mut db = db.lock().await;
                            get.execute(&mut db).await
                        } // _ => RespDataType::errors("unimplemented"),
                    }
                }
                Err(e) => Err(e),
            }
            .unwrap_or_else(|e| RespDataType::errors(e.to_string()));
            dbg!(&ret);

            ret.serialize(&mut stream).await?;
            stream.flush().await?;
        }
    }

    pub async fn serve<A: ToSocketAddrs>(self: Arc<Self>, addr: A) -> crate::util::Result<()> {
        let mut listener = TcpListener::bind(addr).await?;
        while let Some(stream) = listener.incoming().filter_map(|x| x.ok()).next().await {
            let this = self.clone();
            println!(
                "peer connected: {}",
                stream.peer_addr().map_err(|_| "cannot get peer address")?
            );
            tokio::spawn(async move { this.process(stream).await });
        }

        Ok(())
    }
}
