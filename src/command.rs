use crate::data_type::RespDataType;
use crate::util;
use crate::util::{BoxFuture, GenericError};
use core::convert::TryFrom;

#[derive(Debug, Clone)]
pub struct Ping {
    pub message: Option<RespDataType>,
}

#[derive(Debug, Clone)]
pub struct Echo {
    pub message: RespDataType,
}

#[derive(Debug, Clone)]
pub struct Set {
    pub key: RespDataType,
    pub value: RespDataType,
}

#[derive(Debug, Clone)]
pub struct Get {
    pub key: RespDataType,
}

#[derive(Debug, Clone)]
pub enum RespCommand {
    Ping(Ping),
    Echo(Echo),
    Set(Set),
    Get(Get),
}

impl TryFrom<RespDataType> for RespCommand {
    type Error = GenericError;

    fn try_from(value: RespDataType) -> std::result::Result<Self, Self::Error> {
        match value {
            RespDataType::Arrays(Some(value)) => {
                let (command, args) = match &value[..] {
                    [RespDataType::BulkStrings(Some(str)), args @ ..] => (
                        String::from_utf8((**str).as_ref().to_ascii_lowercase())?,
                        args,
                    ),
                    _ => return Err("expected bulk string".into()),
                };

                match command.as_str() {
                    "ping" => {
                        let arg = args
                            .first()
                            .and_then(|arg| {
                                if let RespDataType::BulkStrings(_) = arg {
                                    Some(arg)
                                } else {
                                    None
                                }
                            })
                            .cloned();

                        Ok(RespCommand::Ping(Ping { message: arg }))
                    }
                    "echo" => {
                        let arg = args
                            .first()
                            .map(|arg| {
                                if let RespDataType::BulkStrings(_) = arg {
                                    Ok(arg)
                                } else {
                                    Err::<_, GenericError>("expected bulk string".into())
                                }
                            })
                            .transpose()?
                            .cloned()
                            .ok_or::<GenericError>("missing argument for echo".into())?;

                        Ok(RespCommand::Echo(Echo { message: arg }))
                    }
                    _ => Err("unknown command".into()),
                }
            }
            _ => Err("expected array".into()),
        }
    }
}

pub trait Command {
    fn execute(&self) -> BoxFuture<util::Result<RespDataType>>;
}

impl Command for Ping {
    fn execute(&self) -> BoxFuture<util::Result<RespDataType>> {
        Box::pin(async move {
            Ok(self
                .message
                .clone()
                .unwrap_or(RespDataType::simple_strings("PONG")))
        })
    }
}

impl Command for Echo {
    fn execute(&self) -> BoxFuture<util::Result<RespDataType>> {
        Box::pin(async move { Ok(self.message.clone()) })
    }
}
