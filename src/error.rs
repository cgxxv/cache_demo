use log::warn;
use tokio::sync::mpsc;

/// Not enough data is available to parse a message
#[derive(Debug)]
pub enum Error {
    Incomplete,
    WhatError,
    TryAgain,
    Other(Box<dyn std::error::Error>),
}

impl From<String> for Error {
    fn from(src: String) -> Error {
        Error::Other(src.into())
    }
}

impl From<&str> for Error {
    fn from(src: &str) -> Error {
        Error::Other(src.into())
    }
}

// impl From<std::string::FromUtf8Error> for Error {
//     fn from(src: std::string::FromUtf8Error) -> Error {
//         Error::Other(src.into())
//     }
// }

impl From<std::str::Utf8Error> for Error {
    fn from(src: std::str::Utf8Error) -> Error {
        Error::Other(src.into())
    }
}

impl From<std::io::Error> for Error {
    fn from(src: std::io::Error) -> Error {
        warn!("got std io error {:?}", src);
        Error::Other(src.into())
    }
}

impl<T> From<mpsc::error::SendError<T>> for Error {
    fn from(_: mpsc::error::SendError<T>) -> Error {
        Error::Other("mpsc send error".into())
    }
}

// impl<T> From<broadcast::error::SendError<T>> for Error {
//     fn from(_: broadcast::error::SendError<T>) -> Error {
//         Error::Other("broadcast send error".into())
//     }
// }

// impl From<broadcast::error::RecvError> for Error {
//     fn from(src: broadcast::error::RecvError) -> Self {
//         Error::Other(src.into())
//     }
// }

impl From<tokio::task::JoinError> for Error {
    fn from(src: tokio::task::JoinError) -> Error {
        Error::Other(src.into())
    }
}

impl From<anyhow::Error> for Error {
    fn from(src: anyhow::Error) -> Error {
        Error::Other(src.into())
    }
}

impl From<std::net::AddrParseError> for Error {
    fn from(src: std::net::AddrParseError) -> Error {
        Error::Other(src.into())
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(src: std::num::ParseIntError) -> Error {
        Error::Other(src.into())
    }
}

impl From<serde_json::Error> for Error {
    fn from(src: serde_json::Error) -> Self {
        Error::Other(src.into())
    }
}

impl From<reqwest::Error> for Error {
    fn from(src: reqwest::Error) -> Self {
        Error::Other(src.into())
    }
}

// impl From<()> for Error {
//     fn from(_: ()) -> Self {
//         Error::Other("WTF".into())
//     }
// }

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(fmt),
            Error::WhatError => "invalid arguments".fmt(fmt),
            Error::TryAgain => "try again".fmt(fmt),
            Error::Other(err) => err.fmt(fmt),
        }
    }
}


unsafe impl Send for Error {}
unsafe impl Sync for Error {}
