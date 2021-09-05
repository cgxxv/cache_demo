use std::{io::Cursor, net::SocketAddr};
use std::str;

use bytes::{Buf, BytesMut};
use log::{debug, info};
use tokio::{io::{AsyncReadExt, AsyncWriteExt, BufWriter}, net::{TcpStream}};

use crate::command::Command;
use crate::{Error, Result};

#[derive(Debug)]
pub struct Conn {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Conn {
    pub fn new(socket: TcpStream) -> Self {
        Self {
            stream: BufWriter::new(socket),
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    pub fn peer_addr(&self) -> Result<SocketAddr> {
        let addr = self.stream.get_ref().peer_addr()?;
        info!("{:?}", addr);

        Ok(addr)
    }

    pub async fn read_buf(&mut self) -> Result<Option<Command>> {
        loop {
            if let Some(cmd) = self.parse_command()? {
                return Ok(Some(cmd));
            }

            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    pub async fn write_buf(&mut self, s: String) -> Result<()> {
        debug!("{:?}", s);
        self.stream.write_all(s.as_bytes()).await?;
        Ok(self.stream.flush().await?)
    }

    fn parse_command(&mut self) -> Result<Option<Command>> {
        debug!("{:?}", self.buffer.to_owned());

        let mut buf = Cursor::new(&self.buffer[..]);
        buf.set_position(0);
        match self.get_line(&mut buf) {
            Ok(line) => {
                let len = buf.position() as usize;
                let line = str::from_utf8(line)?;
                let cmd = Command::decode(line)?;
                // debug!("{:?}", cmd.action());

                self.buffer.advance(len + 1);

                Ok(Some(cmd))
            }
            Err(Error::Incomplete) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn get_line<'a>(&self, src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8]> {
        let start = src.position() as usize;
        let end = src.get_ref().len();

        for i in start..end {
            if src.get_ref()[i] == b'\n' {
                src.set_position(i as u64);

                // Return the line
                return Ok(&src.get_ref()[start..i]);
            }
        }

        Err(Error::Incomplete)
    }
}
