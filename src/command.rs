use crate::data_type::RedisDataTypeWithTTL;
use crate::{
    data_type::{RedisDataType, RespDataType},
    util::{self, BoxFuture, GenericError},
};
use std::{
    collections::{
        hash_map::Entry::{Occupied, Vacant},
        HashMap,
    },
    convert::{TryFrom, TryInto},
    time::Duration,
};
use tokio::time::Instant;

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
    pub expiry: Option<Duration>,
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
                    [RespDataType::BulkStrings(Some(str)), args @ ..] => {
                        (String::from_utf8(str.to_ascii_lowercase())?, args)
                    }
                    _ => return Err("expected bulk string".into()),
                };

                match command.as_str() {
                    "ping" => {
                        let message = args
                            .first()
                            .and_then(|x| x.expect_bulk_strings().ok())
                            .cloned();

                        Ok(RespCommand::Ping(Ping { message }))
                    }
                    "echo" => {
                        let message = args
                            .first()
                            .ok_or::<GenericError>("missing argument message".into())?
                            .expect_bulk_strings()
                            .map_err::<GenericError, _>(|_| "message is not bulk string".into())?
                            .clone();

                        Ok(RespCommand::Echo(Echo { message }))
                    }
                    "set" => {
                        let key = args
                            .get(0)
                            .ok_or::<GenericError>("missing argument key".into())?
                            .expect_bulk_strings()
                            .map_err::<GenericError, _>(|_| "key is not bulk string".into())?
                            .clone();
                        let value = args
                            .get(1)
                            .ok_or::<GenericError>("missing argument value".into())?
                            .expect_bulk_strings()
                            .map_err::<GenericError, _>(|_| "value is not bulk string".into())?
                            .clone();
                        let expiry_value = args.get(3);
                        match args.get(2) {
                            None if expiry_value.is_none() => Ok(RespCommand::Set(Set {
                                key,
                                value,
                                expiry: None,
                            })),
                            Some(expiry_type) if expiry_value.is_some() => {
                                let ttl_type = expiry_type
                                    .clone()
                                    .into_bulk_strings()?
                                    .map(String::from_utf8)
                                    .transpose()?
                                    .map(|s| s.to_ascii_lowercase())
                                    .ok_or::<GenericError>("expected PX".into())?;
                                dbg!(&ttl_type);

                                let expiry = expiry_value
                                    .cloned()
                                    .map(|x| x.into_integers())
                                    .transpose()?
                                    .ok_or("expected time")?
                                    .try_into()
                                    .map(Duration::from_millis)?;
                                dbg!(&expiry);

                                match ttl_type.as_str() {
                                    "px" => Ok(RespCommand::Set(Set {
                                        key,
                                        value,
                                        expiry: Some(expiry),
                                    })),
                                    _ => Err("expected PX".into()),
                                }
                            }
                            _ => Err("unexpected argument".into()),
                        }
                    }
                    "get" => {
                        let key = args
                            .get(0)
                            .ok_or::<GenericError>("missing argument key".into())?
                            .expect_bulk_strings()
                            .map_err::<GenericError, _>(|_| "key is not bulk string".into())?
                            .clone();

                        Ok(RespCommand::Get(Get { key }))
                    }
                    _ => Err("unknown command".into()),
                }
            }
            _ => Err("expected array".into()),
        }
    }
}

pub trait Command<'a, C> {
    fn execute(&'a self, context: &'a mut C) -> BoxFuture<'a, util::Result<RespDataType>>;
}

impl<'a> Command<'a, ()> for Ping {
    fn execute(&'a self, _: &'a mut ()) -> BoxFuture<'a, util::Result<RespDataType>> {
        Box::pin(async move {
            Ok(self
                .message
                .clone()
                .unwrap_or(RespDataType::simple_strings("PONG")))
        })
    }
}

impl<'a> Command<'a, ()> for Echo {
    fn execute(&'a self, _: &'a mut ()) -> BoxFuture<'a, util::Result<RespDataType>> {
        Box::pin(async move { Ok(self.message.clone()) })
    }
}

impl<'a> Command<'a, HashMap<String, RedisDataTypeWithTTL>> for Set {
    fn execute(
        &'a self,
        context: &'a mut HashMap<String, RedisDataTypeWithTTL>,
    ) -> BoxFuture<'a, util::Result<RespDataType>> {
        Box::pin(async move {
            let key = self
                .key
                .clone()
                .into_bulk_strings()?
                .ok_or::<GenericError>("empty key".into())?;
            let key = String::from_utf8(key).map_err::<GenericError, _>(|x| x.into())?;
            let value: RedisDataType = self.value.clone().try_into()?;
            let entry = context.entry(key);

            let ttl = match self.expiry {
                Some(time) => RedisDataTypeWithTTL::Finite(value, Instant::now() + time),
                None => RedisDataTypeWithTTL::Infinite(value),
            };

            match entry {
                Occupied(mut o) => {
                    // let old_value: RespDataType = o.get().clone().try_into()?;
                    // *o.get_mut() = value;
                    // Ok(old_value)

                    *o.get_mut() = ttl;
                    Ok(RespDataType::simple_strings("OK"))
                }
                Vacant(v) => {
                    v.insert(ttl);
                    Ok(RespDataType::simple_strings("OK"))
                }
            }
        })
    }
}

impl<'a> Command<'a, HashMap<String, RedisDataTypeWithTTL>> for Get {
    fn execute(
        &'a self,
        context: &'a mut HashMap<String, RedisDataTypeWithTTL>,
    ) -> BoxFuture<'a, util::Result<RespDataType>> {
        Box::pin(async move {
            let key = self
                .key
                .clone()
                .into_bulk_strings()?
                .ok_or::<GenericError>("empty key".into())?;
            let key = String::from_utf8(key).map_err::<GenericError, _>(|x| x.into())?;
            match context.get(&key).cloned() {
                Some(RedisDataTypeWithTTL::Infinite(RedisDataType::Strings(s))) => {
                    Ok(RespDataType::bulk_strings(s))
                }
                Some(RedisDataTypeWithTTL::Finite(RedisDataType::Strings(s), ttl)) => {
                    if Instant::now() > ttl {
                        context.remove(&key);
                        Ok(RespDataType::empty_bulk_strings())
                    } else {
                        Ok(RespDataType::bulk_strings(s))
                    }
                }
                Some(_) => Err("entry is not a bulk string".into()),
                None => Ok(RespDataType::empty_bulk_strings()),
            }
        })
    }
}
