use std::io::{Error, ErrorKind};
use std::net::IpAddr;
use std::time::Duration;

use chrono::prelude::Local;
use log::debug;
use tokio::{net::TcpStream, sync::oneshot, time::timeout};

use crate::command::Command;
use crate::conn::Conn;
use crate::{Result, PERSIST_PORT};

#[derive(Debug)]
pub struct Client {
    pub ip: IpAddr,
    pub last_update: i64, //最后一次更新时间戳
    pub last_ping: i64,   //最后一次接受ping的时间
    pub offset: usize,    //数据同步偏移量
    pub birthtime: i64,   //记录client产生时间
    // pub maybe_offline: bool, //主动发现是否离线
    // pub real_offline: bool,  //被动发现是否离线
    // pub leader: bool,  //是否是组长，如果是正常情况，组长发送指令同步数据
    conn: Conn,
}

pub async fn connect(ip: IpAddr) -> Result<Client> {
    let socket = TcpStream::connect(format!("{:?}:{}", ip, PERSIST_PORT)).await?;
    let conn = Conn::new(socket);

    let now = Local::now().timestamp_millis();
    Ok(Client {
        ip,
        last_update: now,
        last_ping: now,
        offset: 0,
        birthtime: now,
        conn,
    })
}

impl Client {
    pub async fn write(&mut self, cmd: Command) -> Result<Command> {
        let (_, rx) = oneshot::channel::<()>();
        let res = tokio::select! {
            res = async {
                self.conn.write_buf(cmd.clone().encode()).await?;
                self.read_response().await
            } => res?,
            Err(_) = timeout(Duration::from_millis(50), rx) => return Err(format!("client write timeout {:?}", cmd).into()),
        };

        Ok(res)
    }

    pub async fn ping(&mut self) -> Result<Command> {
        self.conn.write_buf(Command::ping().encode()).await?;
        self.read_response().await
    }

    pub async fn ping_pong(&mut self) -> bool {
        debug!("ping_pong");
        let (_, rx) = oneshot::channel::<()>();

        tokio::select! {
            res = self.ping() => {
                match res {
                    Ok(res) => res.is_pong(),
                    Err(_) => false,
                }
            },
            Err(_) = timeout(Duration::from_millis(50), rx) => false,
        }
    }

    pub async fn check(&mut self, ip: String, kind: String) -> Result<Command> {
        self.conn
            .write_buf(Command::check(ip, kind).encode())
            .await?;
        self.read_response().await
    }

    pub async fn sync1(&mut self, cmd: Command) -> Result<Command> {
        self.conn.write_buf(Command::sync1(cmd).encode()).await?;
        self.read_response().await
    }

    pub async fn psync(&mut self, cmd: String) -> Result<Command> {
        self.conn.write_buf(Command::psync(cmd).encode()).await?;
        self.read_response().await
    }

    pub async fn info(&mut self, ips: String) -> Result<Command> {
        self.conn.write_buf(Command::info(ips).encode()).await?;
        self.read_response().await
    }

    pub async fn loip(&mut self, ip: String) -> Result<Command> {
        self.conn.write_buf(Command::loip(ip).encode()).await?;
        self.read_response().await
    }

    pub async fn weak(&mut self) -> Result<Command> {
        self.conn.write_buf(Command::weak().encode()).await?;
        self.read_response().await
    }

    /*
    offset: usize,       //数据同步偏移量
    last_update: i64,    //最后一次更新时间戳
    last_ping: i64,      //最后一次接受ping的时间
    birthtime: i64,      //记录产生时间
    */
    pub async fn vote_me(&mut self) -> Result<(usize, i64, i64, i64, IpAddr)> {
        self.conn
            .write_buf(Command::vote(self.ip.to_string()).encode())
            .await?;
        let cmd = self.read_response().await?;
        if !cmd.is_meta() {
            return Err(format!("unknown command {:?}", cmd).into());
        }

        let v = cmd.key.unwrap();
        let v = v.split('-').collect::<Vec<&str>>();
        let offset = v[0].parse::<usize>()?;
        let last_update = v[1].parse::<i64>()?;
        let last_ping = v[2].parse::<i64>()?;
        let birthtime = v[3].parse::<i64>()?;

        Ok((offset, last_update, last_ping, birthtime, self.ip))
    }

    pub async fn set_master(&mut self, ip: String) -> Result<Command> {
        self.conn.write_buf(Command::master(ip).encode()).await?;
        self.read_response().await
    }

    pub async fn append(&mut self, cmd: Command) -> Result<Command> {
        self.conn.write_buf(Command::append(cmd).encode()).await?;
        self.read_response().await
    }

    async fn read_response(&mut self) -> Result<Command> {
        let response = self.conn.read_buf().await?;

        match response {
            Some(cmd) => {
                // debug!("{:?}", cmd.action());

                let now = Local::now().timestamp_millis();
                self.last_update = now;
                if cmd.is_pong() {
                    self.last_ping = now;
                }

                Ok(cmd)
            }
            None => {
                let err = Error::new(ErrorKind::ConnectionReset, "connection reset by server");
                Err(err.into())
            }
        }
    }
}
