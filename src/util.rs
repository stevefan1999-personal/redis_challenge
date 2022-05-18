use std::error;
use std::future::Future;
use std::pin::Pin;

pub fn strip_trailing_newline(input: &str) -> &str {
    input
        .strip_suffix("\r\n")
        .or(input.strip_suffix("\n"))
        .unwrap_or(input)
}

pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a + Send + Sync>>;
pub(crate) type GenericError = Box<dyn error::Error + Send + Sync>;
pub(crate) type Result<T> = std::result::Result<T, GenericError>;
