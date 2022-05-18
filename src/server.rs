use std::convert::TryInto;

use tokio::io::{AsyncWriteExt, BufStream};
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::stream::StreamExt;

use crate::command::{Command, RespCommand};
use crate::data_type::RespDataType;
use crate::util::GenericError;

pub struct RedisServer {
    listener: TcpListener,
}

impl RedisServer {
    pub async fn new<A: ToSocketAddrs>(address: A) -> crate::util::Result<RedisServer> {
        Ok(RedisServer {
            listener: TcpListener::bind(address).await?,
        })
    }

    pub async fn serve(&mut self) -> crate::util::Result<()> {
        while let Some(stream) = self.listener.incoming().filter_map(|x| x.ok()).next().await {
            println!(
                "peer connected: {}",
                stream.peer_addr().map_err(|_| "cannot get peer address")?
            );
            tokio::spawn(async move {
                let mut stream = BufStream::new(stream);
                loop {
                    let ret = match RespDataType::deserialize(&mut stream)
                        .await
                        .and_then(|data| data.try_into() as Result<RespCommand, _>)
                    {
                        Ok(cmd) => {
                            dbg!(&cmd);
                            match cmd {
                                RespCommand::Ping(ping) => ping.execute().await?,
                                RespCommand::Echo(echo) => echo.execute().await?,
                                _ => RespDataType::errors("unimplemented"),
                            }
                        }
                        Err(e) => {
                            dbg!(&e);
                            RespDataType::errors(e.to_string())
                        }
                    };
                    dbg!(&ret);

                    ret.serialize(&mut stream).await?;
                    stream.flush().await?;
                }
                Ok::<_, GenericError>(())
            });
        }

        Ok(())
    }
}
