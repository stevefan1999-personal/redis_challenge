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
};

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
                        Ok(RespCommand::Set(Set { key, value }))
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

impl<'a> Command<'a, HashMap<String, RedisDataType>> for Set {
    fn execute(
        &'a self,
        context: &'a mut HashMap<String, RedisDataType>,
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

            match entry {
                Occupied(mut o) => {
                    // let old_value: RespDataType = o.get().clone().try_into()?;
                    // *o.get_mut() = value;
                    // Ok(old_value)
                    *o.get_mut() = value;
                    Ok(RespDataType::simple_strings("OK"))
                }
                Vacant(v) => {
                    v.insert(value);
                    Ok(RespDataType::simple_strings("OK"))
                }
            }
        })
    }
}

impl<'a> Command<'a, HashMap<String, RedisDataType>> for Get {
    fn execute(
        &'a self,
        context: &'a mut HashMap<String, RedisDataType>,
    ) -> BoxFuture<'a, util::Result<RespDataType>> {
        Box::pin(async move {
            let key = self
                .key
                .clone()
                .into_bulk_strings()?
                .ok_or::<GenericError>("empty key".into())?;
            let key = String::from_utf8(key).map_err::<GenericError, _>(|x| x.into())?;
            match context.get(&key).cloned() {
                Some(RedisDataType::Strings(s)) => Ok(RespDataType::bulk_strings(s)),
                Some(_) => Err("entry is not a bulk string".into()),
                None => Ok(RespDataType::empty_bulk_strings()),
            }
        })
    }
}
