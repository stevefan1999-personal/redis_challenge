use crate::util::BoxFuture;
use tokio::prelude::*;

#[derive(Debug, Clone, PartialEq)]
pub enum RespDataType {
    SimpleStrings(Vec<u8>),
    Errors(Vec<u8>),
    Integers(i64),
    BulkStrings(Option<Vec<u8>>),
    Arrays(Option<Vec<RespDataType>>),
}

impl RespDataType {
    pub fn simple_strings<S: AsRef<[u8]>>(str: S) -> RespDataType {
        RespDataType::SimpleStrings(str.as_ref().to_owned())
    }

    pub fn errors<S: AsRef<[u8]>>(str: S) -> RespDataType {
        RespDataType::Errors(str.as_ref().to_owned())
    }

    pub fn integers(num: i64) -> RespDataType {
        RespDataType::Integers(num)
    }

    pub fn bulk_strings<S: AsRef<[u8]>>(str: S) -> RespDataType {
        RespDataType::BulkStrings(Some(str.as_ref().to_owned()))
    }

    pub fn empty_bulk_strings() -> RespDataType {
        RespDataType::BulkStrings(None)
    }

    pub fn arrays(vec: Vec<RespDataType>) -> RespDataType {
        RespDataType::Arrays(Some(vec))
    }

    pub fn empty_arrays() -> RespDataType {
        RespDataType::Arrays(None)
    }

    pub fn tag(&self) -> u8 {
        match self {
            Self::SimpleStrings(_) => b'+',
            Self::Errors(_) => b'-',
            Self::Integers(_) => b':',
            Self::BulkStrings(_) => b'$',
            Self::Arrays(_) => b'*',
        }
    }
}

impl RespDataType {
    pub fn serialize<'a, S: AsyncWrite + Unpin + Send + Sync>(
        &'a self,
        sink: &'a mut S,
    ) -> BoxFuture<'a, crate::util::Result<()>> {
        Box::pin(async move {
            sink.write_u8(self.tag()).await?;
            match self {
                Self::SimpleStrings(str) => {
                    dbg!(str);
                    sink.write(str).await?;
                    sink.write(b"\r\n").await?;
                }
                Self::Errors(err) => {
                    dbg!(err);
                    sink.write(err).await?;
                    sink.write(b"\r\n").await?;
                }
                Self::Integers(num) => {
                    dbg!(num);
                    sink.write(num.to_string().as_bytes()).await?;
                    sink.write(b"\r\n").await?;
                }
                Self::BulkStrings(str) => {
                    dbg!(str);
                    match str {
                        Some(str) => {
                            dbg!(str);
                            sink.write(str.len().to_string().as_bytes()).await?;
                            sink.write(b"\r\n").await?;
                            sink.write(str).await?;
                        }
                        None => {
                            sink.write(b"-1").await?;
                        }
                    }
                    sink.write(b"\r\n").await?;
                }
                Self::Arrays(Some(arr)) => {
                    dbg!(arr);
                    sink.write(arr.len().to_string().as_bytes()).await?;
                    sink.write(b"\r\n").await?;
                    for elem in arr {
                        elem.serialize(sink).await?;
                    }
                }

                Self::Arrays(None) => {
                    dbg!("array none");
                    sink.write(b"-1").await?;
                    sink.write(b"\r\n").await?;
                }
            };

            Ok(())
        })
    }

    pub fn deserialize<S: AsyncBufRead + Unpin + Send + Sync>(
        source: &mut S,
    ) -> BoxFuture<crate::util::Result<RespDataType>> {
        async fn expect_new_line<S: AsyncBufRead + std::marker::Unpin>(
            source: &mut S,
        ) -> crate::util::Result<()> {
            let mut newline: [u8; 2] = [0; 2];
            source.read_exact(&mut newline).await?;
            if &newline != b"\r\n" {
                Err(format!("expected <CR> <LF>, got {:?}", newline).into())
            } else {
                Ok(())
            }
        }

        Box::pin(async move {
            match source.read_u8().await? {
                b'+' => {
                    dbg!("read simple strings");
                    let mut buf = String::new();
                    source.read_line(&mut buf).await?;
                    let buf = crate::util::strip_trailing_newline(&buf).to_owned();
                    dbg!(&buf);
                    Ok(RespDataType::SimpleStrings(buf.into_bytes()))
                }
                b'-' => {
                    dbg!("read errors");
                    let mut buf = String::new();
                    source.read_line(&mut buf).await?;
                    let buf = crate::util::strip_trailing_newline(&buf).to_owned();
                    dbg!(&buf);
                    Ok(RespDataType::Errors(buf.into_bytes()))
                }
                b':' => {
                    dbg!("read integers");
                    let mut buf = String::new();
                    source.read_line(&mut buf).await?;
                    let buf = crate::util::strip_trailing_newline(&buf).to_owned();
                    dbg!(&buf);
                    let num = buf.parse()?;
                    dbg!(num);
                    Ok(RespDataType::Integers(num))
                }
                b'$' => {
                    dbg!("read bulk strings");
                    let mut buf = String::new();
                    source.read_line(&mut buf).await?;
                    let buf = crate::util::strip_trailing_newline(&buf).to_owned();
                    dbg!(&buf);
                    let n = buf.parse::<i64>()?;
                    dbg!(n);
                    Ok(RespDataType::BulkStrings(if n >= 0 {
                        let mut str = vec![0; n as usize];
                        source.read_exact(&mut str).await?;
                        dbg!(&str);
                        expect_new_line(source).await?;
                        Some(str)
                    } else {
                        None
                    }))
                }
                b'*' => {
                    dbg!("read arrays");
                    let mut buf = String::new();
                    source.read_line(&mut buf).await?;
                    let buf = crate::util::strip_trailing_newline(&buf).to_owned();
                    let n = buf.parse::<i64>()?;
                    Ok(RespDataType::Arrays(if n >= 0 {
                        let mut vec = Vec::with_capacity(n as usize);
                        for _ in 0..n {
                            let deser = Self::deserialize(source).await?;
                            dbg!(&deser);
                            vec.push(deser);
                        }
                        dbg!(&vec);
                        Some(vec)
                    } else {
                        None
                    }))
                }
                x => Err(format!("unknown message tag '{}'", x).into()),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::RespDataType;
    use crate::util::Result;
    use bytes::BytesMut;
    use tokio::io::BufStream;
    use tokio::prelude::*;

    async fn assert_idempotent(data: RespDataType, expected: &str) -> Result<()> {
        let (mut client, server) = tokio::io::duplex(4096);
        let mut server = BufStream::new(server);
        client.write(expected.as_bytes()).await?;
        let server_read_data = RespDataType::deserialize(&mut server).await?;
        assert_eq!(data, server_read_data);

        data.serialize(&mut server).await?;
        server.flush().await?;

        let mut buf = BytesMut::with_capacity(4096);
        client.read_buf(&mut buf).await?;
        assert_eq!(buf, expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_simple_strings() -> Result<()> {
        assert_idempotent(RespDataType::SimpleStrings(Box::new("OK")), "+OK\r\n").await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_errors() -> Result<()> {
        assert_idempotent(RespDataType::errors("Error message"), "-Error message\r\n").await?;
        assert_idempotent(
            RespDataType::errors("ERR unknown command 'helloworld'"),
            "-ERR unknown command 'helloworld'\r\n",
        )
        .await?;
        assert_idempotent(
            RespDataType::errors(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            ),
            "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_integers() -> Result<()> {
        assert_idempotent(RespDataType::integers(-1), ":-1\r\n").await?;
        assert_idempotent(RespDataType::integers(0), ":0\r\n").await?;
        assert_idempotent(RespDataType::integers(1000), ":1000\r\n").await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_bulk_strings() -> Result<()> {
        assert_idempotent(RespDataType::bulk_strings("hello"), "$5\r\nhello\r\n").await?;
        assert_idempotent(RespDataType::bulk_strings(""), "$0\r\n\r\n").await?;
        assert_idempotent(RespDataType::empty_bulk_strings(), "$-1\r\n").await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_arrays() -> Result<()> {
        assert_idempotent(RespDataType::arrays(vec![]), "*0\r\n").await?;
        assert_idempotent(
            RespDataType::arrays(vec![RespDataType::bulk_strings("hello")]),
            "*1\r\n$5\r\nhello\r\n",
        )
        .await?;
        assert_idempotent(
            RespDataType::arrays(vec![
                RespDataType::bulk_strings("hello"),
                RespDataType::bulk_strings("world"),
            ]),
            "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n",
        )
        .await?;
        assert_idempotent(
            RespDataType::arrays(vec![
                RespDataType::integers(1),
                RespDataType::integers(2),
                RespDataType::integers(3),
            ]),
            "*3\r\n:1\r\n:2\r\n:3\r\n",
        )
        .await?;
        assert_idempotent(
            RespDataType::arrays(vec![
                RespDataType::integers(1),
                RespDataType::integers(2),
                RespDataType::integers(3),
                RespDataType::integers(4),
                RespDataType::bulk_strings("hello"),
            ]),
            "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$5\r\nhello\r\n",
        )
        .await?;
        assert_idempotent(RespDataType::empty_arrays(), "*-1\r\n").await?;

        assert_idempotent(
            RespDataType::arrays(vec![
                RespDataType::arrays(vec![
                    RespDataType::integers(1),
                    RespDataType::integers(2),
                    RespDataType::integers(3),
                ]),
                RespDataType::arrays(vec![
                    RespDataType::simple_strings("Hello"),
                    RespDataType::errors("World"),
                ]),
            ]),
            "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Hello\r\n-World\r\n",
        )
        .await?;
        Ok(())
    }
}
