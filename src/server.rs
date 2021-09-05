use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use std::vec;
use std::{net::IpAddr, str::FromStr};

use chrono::prelude::Local;
use log::{debug, info, warn};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{broadcast, mpsc, oneshot},
    task::JoinHandle,
    time::{self, timeout},
};

use crate::client;
use crate::cluster::Cluster;
use crate::command::{Command, Field};
use crate::conn::Conn;
use crate::models::{self, Db};
use crate::shutdown::Shutdown;
use crate::Result;

pub async fn notify(db: Db, mut sync_rx: mpsc::Receiver<Command>) -> Result<JoinHandle<()>> {
    let t = tokio::spawn(async move {
        while let Some(cmd) = sync_rx.recv().await {
            debug!("===> notify {:?}", cmd);
            if cmd.is_quit() {
                debug!("receive notify `quit` command");
                break;
            }

            if let Err(e) = process(Arc::clone(&db), cmd).await {
                warn!("WTF error: {:?}", e);
            }
        }

        info!("exit notify safely");
    });

    Ok(t)
}

pub struct Server {
    db: Db,
    buf_tx: mpsc::Sender<Command>,
    listener: TcpListener,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

impl Server {
    pub async fn new(db: Db, buf_tx: mpsc::Sender<Command>) -> Result<Self> {
        let listener = TcpListener::bind("0.0.0.0:8081").await?;

        let (notify_shutdown, _) = broadcast::channel(1);
        let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

        // let mut hc = HealthCheck {
        //     db: Arc::clone(&db),
        //     shutdown: Shutdown::new(notify_shutdown.subscribe()),
        //     _shutdown_complete: shutdown_complete_tx.clone(),
        // };

        // tokio::spawn(async move {
        //     info!("health checking started");
        //     if let Err(e) = hc.run().await {
        //         warn!("error occured: {:?}", e);
        //     }
        // });

        let mut bc = BackCheck {
            db: Arc::clone(&db),
            shutdown: Shutdown::new(notify_shutdown.subscribe()),
            _shutdown_complete: shutdown_complete_tx.clone(),
        };

        tokio::spawn(async move {
            info!("back checking started");
            if let Err(e) = bc.run().await {
                warn!("error occured: {:?}", e);
            }
        });

        Ok(Self {
            db,
            buf_tx,
            listener,
            notify_shutdown,
            shutdown_complete_rx,
            shutdown_complete_tx,
        })
    }

    pub async fn persist(
        mut self,
        mut cluster_quit_rx: mpsc::UnboundedReceiver<()>,
    ) -> Result<JoinHandle<()>> {
        let t = tokio::spawn(async move {
            tokio::select! {
                res = self.run() => {
                    if let Err(e) = res {
                        warn!("failed to accept, caused by: {:?}", e);
                    }
                }
                _ = cluster_quit_rx.recv() => {
                    debug!("cluster persist exiting");
                },
            };

            drop(self.notify_shutdown);
            drop(self.shutdown_complete_tx);

            let _ = self.shutdown_complete_rx.recv().await;
            debug!("<= cluster persist exit safely");
        });
        info!("cluster persist started");

        Ok(t)
    }

    #[allow(dead_code)]
    async fn accept(&mut self) -> Result<TcpStream> {
        let mut backoff = 1;

        // Try to accept a few times
        loop {
            // Perform the accept operation. If a socket is successfully
            // accepted, return it. Otherwise, save the error.
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > 64 {
                        // Accept has failed too many times. Return the error.
                        warn!("listener accept error {:?}", err);
                        return Err(err.into());
                    }
                }
            }

            // Pause execution until the back off period elapses.
            time::sleep(Duration::from_secs(backoff)).await;

            // Double the back off
            backoff *= 2;
        }
    }

    async fn run(&mut self) -> Result<()> {
        loop {
            let socket = self.accept().await?;
            // let (socket, _) = self.listener.accept().await?;
            debug!("create socket");

            let mut handler = Handler {
                db: Arc::clone(&self.db),
                buf_tx: self.buf_tx.clone(),
                conn: Conn::new(socket),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            tokio::spawn(async move {
                debug!("data handling started");
                if let Err(e) = handler.run().await {
                    warn!("error occured: {:?}", e);
                }
            });
        }
    }
}

async fn process(db: Db, cmd: Command) -> Result<()> {
    //slave是不可能直接收到指令的
    info!("=> {:?}", cmd);

    //缓存指令为了向重新上线的slave同步数据
    for lq in db.lagcmd.lock().await.values_mut() {
        lq.push_back(cmd.clone());
    }
    //如果是master, 直接执行sync_slaves
    //如果是slave, 必须首先通知master, 然后再通过master持久化自己sync_slaves
    let (ipaddrs, is_master) = match db.master_client.write().await.as_mut() {
        Some(master) => {
            //不可能到这里，因为slave的所有写入指令都需通过sync1包装，并由master下发
            info!("====> I am slave");
            let ipaddrs = Cluster::notify_master(master, cmd.clone()).await?;
            (ipaddrs, true)
        }
        None => {
            info!("====> I am master");
            let mut slaves_client = db.slaves_client.write().await;
            let mut v = vec![];
            for slave in slaves_client.values_mut() {
                info!("==> start {:?}", slave.ip);
                // if !Cluster::notify_slave(slave, cmd.clone()).await? {
                //     v.push(slave.ip);
                // }

                match Cluster::notify_slave(slave, cmd.clone()).await {
                    Ok(res) if !res => {
                        warn!("failed {:?}", slave.ip);
                        v.push(slave.ip)
                    }
                    Ok(_) => info!(":)"),
                    Err(e) => warn!("WTF {:?}", e),
                }
                info!("<== end {:?}", slave.ip);
            }
            (v, false)
        }
    };
    info!("<= {:?} {}", ipaddrs, is_master);

    //如果master同步失败，则通知其他slave，选举新的leader
    //如果slave同步失败，直接删除slave_client
    if ipaddrs.is_empty() {
        let mut cluster = db.cluster.write().await;
        cluster.last_update = Local::now().timestamp_millis();
        cluster.offset += std::mem::size_of_val(&cmd);
    } else {
        //这里应该直接进入故障检测与转移
        //TODO:
        //如果是slave同步失败，则简单剔除对应的slave连接就行
        //如果是master接受同步失败，就很麻烦了
        transfer(db, ipaddrs, is_master, cmd.clone()).await?;
    }

    Ok(())
}

async fn transfer(db: Db, ipaddrs: Vec<IpAddr>, is_master: bool, cmd: Command) -> Result<()> {
    info!("TRANSFER started {:?}", cmd);
    let mut cluster = db.cluster.write().await;
    if is_master {
        info!("TRANSFER started at master branch");
        //不可能到这里
        //证明当前集群节点是slave
        let ipaddr = ipaddrs[0];
        let mut lagcmd = db.lagcmd.lock().await;
        if let Some(lq) = lagcmd.get_mut(&ipaddr) {
            lq.push_back(cmd);
        } else {
            let mut lq = VecDeque::new();
            lq.push_back(cmd);
            lagcmd.insert(ipaddr, lq);
        }
        drop(lagcmd);

        let mut slaves_client = db.slaves_client.write().await;
        let mut offline_count = 0;
        for slave in slaves_client.values_mut() {
            let cmd = slave.check(ipaddr.to_string(), MASTER.into()).await?;
            if cmd.is_offline() {
                offline_count += 1;
            }
        }

        if offline_count > (cluster.connected_slaves - 1) / 2 {
            *db.master_client.write().await = None;
            cluster.master = None;
            cluster.watching.push(ipaddr);
        }

        cluster.failover = true;

        let mut metas: Vec<(usize, i64, i64, i64, IpAddr)> = vec![];
        for slave in slaves_client.values_mut() {
            metas.push(slave.vote_me().await?);
        }

        cluster.birthtime = Local::now().timestamp_millis();
        metas.push((
            cluster.offset,
            cluster.last_update,
            cluster.last_ping,
            cluster.birthtime,
            cluster.local_ip.unwrap(),
        ));
        metas.sort();
        let new_master_ipaddr = metas.last().unwrap().4;
        info!("TRANSFER new master {:?} born", new_master_ipaddr);
        for slave in slaves_client.values_mut() {
            slave.set_master(new_master_ipaddr.to_string()).await?;
        }
        info!("TRANSFER end as master branch");
    } else {
        info!("TRANSFER started at slave branch");
        let mut slaves_client = db.slaves_client.write().await;
        //当前集群节点是master
        for ipaddr in ipaddrs {
            if let Some(slave) = slaves_client.get_mut(&ipaddr) {
                //为了尽最大可能保留slave的响应能力，所以这里标记slave为weak弱网络状态
                //当集群被标记为weak的时候，集群应该主动把自己的http请求转发给对应的master或者slave
                slave.weak().await?;
            }

            slaves_client.remove(&ipaddr);
            cluster.slaves.remove(&ipaddr);
            cluster.watching.push(ipaddr);
            cluster.connected_slaves -= 1;
        }
        info!("TRANSFER end at slave branch");
    }
    info!("FIN TRANSFER");

    Ok(())
}

struct Handler {
    db: Db,
    buf_tx: mpsc::Sender<Command>,
    conn: Conn,
    shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
}

static SLAVE: &str = "slave";
static MASTER: &str = "master";

impl Handler {
    async fn run(&mut self) -> Result<()> {
        while !self.shutdown.is_shutdown() {
            debug!("read from socket");

            let res = tokio::select! {
                res = self.conn.read_buf() => {
                    debug!(">> recv from connection");
                    res?
                },
                _ = self.shutdown.recv() => {
                    debug!("<= exit handler safely");
                    return Ok(())
                },
            };
            info!("{:?}", res);

            match res {
                Some(cmd) => {
                    info!("{:?}", cmd.action());

                    if cmd.is_pong() {
                        warn!("unexpected {:?}", cmd);
                    } else if cmd.is_pass() {
                        warn!("unexpected {:?}", cmd);
                    } else if cmd.is_offline() {
                        warn!("unexpected {:?}", cmd);
                    } else if cmd.is_psfin() {
                        warn!("unexpected {:?}", cmd);
                    } else if cmd.is_nop() {
                        warn!("unexpected {:?}", cmd);
                    } else if cmd.is_response() {
                        warn!("unexpected {:?}", cmd);
                    } else if cmd.is_master() {
                        info!("MASTER {:?}", cmd);
                        //接受到被动设置master请求
                        //master_ipaddr
                        let ipaddr = IpAddr::from_str(cmd.key.unwrap().as_str())?;
                        let mut cluster = self.db.cluster.write().await;
                        cluster.master = Some(ipaddr);
                        cluster.slaves.remove(&ipaddr);
                        cluster.connected_slaves -= 1;
                        cluster.failover = false;
                        let local_ip = cluster.local_ip;
                        drop(cluster);

                        let mut master_client = self.db.master_client.write().await;
                        if local_ip != Some(ipaddr) {
                            info!("i am not master, my ip {:?}", local_ip);
                            //我不是master
                            match client::connect(ipaddr).await {
                                Ok(client) => *master_client = Some(client),
                                Err(e) => warn!("err: {:?}, {:?}", e, &ipaddr),
                            }
                            drop(master_client);
                            self.db.slaves_client.write().await.remove(&ipaddr);
                        } else {
                            info!("i am master, my ip {:?}", local_ip);
                            //别人主动把我设置为master
                            *master_client = None;
                            drop(master_client);
                            self.db.cluster.write().await.leader = true;
                            //TODO: 测试新的网络环境
                        }
                        self.conn.write_buf(Command::nop().encode()).await?;
                        info!("FIN MASTER, new master {:?} born", ipaddr);
                    } else if cmd.is_vote() {
                        info!("VOTE {:?}", cmd);
                        let mut cluster = self.db.cluster.write().await;
                        cluster.birthtime = Local::now().timestamp_millis();
                        let offset = cluster.offset;
                        let last_update = cluster.last_update;
                        let last_ping = cluster.last_ping;
                        let birthtime = cluster.birthtime;
                        drop(cluster);

                        self.conn
                            .write_buf(
                                Command::meta(offset, last_update, last_ping, birthtime).encode(),
                            )
                            .await?;

                        info!("FIN VOTE");
                    } else if cmd.is_ping() {
                        info!("PING {:?}", cmd);
                        let mut cluster = self.db.cluster.write().await;
                        cluster.last_ping = Local::now().timestamp_millis();
                        //收到ping，标记recover=false
                        cluster.recover = false;
                        self.conn.write_buf(Command::pong().encode()).await?;
                        info!("FIN PING");
                    } else if cmd.is_check() {
                        //收到其他机器发来的检查指令
                        let ipaddr = IpAddr::from_str(cmd.key.unwrap().as_str())?;
                        let kind = cmd.val.unwrap().3;
                        self.failover(ipaddr, kind.as_str(), false).await?;
                    } else if cmd.is_info() {
                        info!("INFO {:?}", cmd);
                        let ips = cmd.key.unwrap();
                        let ips = ips.split(',').collect::<Vec<&str>>();
                        let mut cluster = self.db.cluster.write().await;
                        let mut slaves_client = self.db.slaves_client.write().await;
                        let mut lagcmd = self.db.lagcmd.lock().await;
                        for ip in ips {
                            let ipaddr = IpAddr::from_str(ip)?;
                            match client::connect(ipaddr).await {
                                Ok(client) => {
                                    slaves_client.insert(ipaddr, client);
                                    if let Some(lq) = lagcmd.get_mut(&ipaddr) {
                                        lq.clear();
                                    }
                                }
                                Err(e) => {
                                    warn!("failed to connect {:?} error {:?}", ipaddr, e);
                                    continue;
                                }
                            };

                            cluster.slaves.insert(ipaddr, ());
                            cluster.connected_slaves += 1;
                        }
                        self.conn.write_buf(Command::nop().encode()).await?;
                    } else if cmd.is_loip() {
                        info!("LOIP {:?}", cmd);
                        let ip = cmd.key.unwrap();
                        let ipaddr = IpAddr::from_str(&ip)?;
                        for slave in self.db.slaves_client.write().await.values_mut() {
                            slave.info(ipaddr.to_string()).await?;
                        }

                        let mut cluster = self.db.cluster.write().await;
                        cluster.local_ip = Some(ipaddr);
                        cluster.birthtime = Local::now().timestamp_millis();
                        cluster.recover = true;
                        self.conn.write_buf(Command::nop().encode()).await?;
                        info!("FIN LOIP");
                    } else if cmd.is_append() {
                        //master向slave发送的指令
                        //restore
                        //persist
                        let cmd = cmd.val.unwrap().3;
                        let cmd = Command::decode(&cmd)?;

                        //缓存指令为了向重新上线的slave同步数据
                        //这里为了尽快的将数据同步，所以在master或者slave下线的时间内
                        //所有的master，slave均保存一份数据用于更快的恢复复活的机器
                        for lq in self.db.lagcmd.lock().await.values_mut() {
                            lq.push_back(cmd.clone());
                        }

                        debug!("APPEND {:?}", cmd);
                        cmd.clone().restore(&self.db); //slave收到指令，刷入缓存中
                        debug!("RESTORED");
                        self.buf_tx.send(cmd.clone()).await?; //slave持久化
                        debug!("PERSISTED");
                        self.conn.write_buf(Command::nop().encode()).await?; //slave返回结果给master
                        info!("FIN APPEND");
                    } else if cmd.is_sync1() {
                        info!("SYNC1 {:?}", cmd);
                        //某个slave向master发送的指令
                        //restore
                        //sync to slaves
                        //persist
                        let cmd = cmd.val.unwrap().3;
                        let cmd = Command::decode(&cmd)?;

                        for lq in self.db.lagcmd.lock().await.values_mut() {
                            lq.push_back(cmd.clone());
                        }

                        cmd.clone().restore(&self.db); //master收到指令，刷入缓存
                        let mut slaves_client = self.db.slaves_client.write().await;
                        for slave in slaves_client.values_mut() {
                            slave.append(cmd.clone()).await?; //master将指令通知，slave执行append
                        }
                        self.buf_tx.send(cmd.clone()).await?; //master持久化
                        self.conn.write_buf(Command::nop().encode()).await?; //master返回结果给slave
                        info!("FIN SYNC1");
                    } else if cmd.is_query() {
                        info!("QUERY {:?}", cmd);
                        let res = match cmd.name {
                            Some(Field::query_create_room) => {
                                models::create_room(cmd.key.unwrap().as_str(), &self.db).await
                            }
                            Some(Field::query_enter_room) => {
                                models::enter_room(cmd.key.unwrap(), &self.db).await
                            }
                            Some(Field::query_leave_room) => {
                                models::leave_room(cmd.key.unwrap().as_str(), &self.db).await
                            }
                            Some(Field::query_room_info) => {
                                models::room_info(cmd.key.unwrap(), &self.db).await
                            }
                            Some(Field::query_room_users) => {
                                models::room_users(cmd.key.unwrap(), &self.db).await
                            }
                            Some(Field::query_room_lists) => {
                                models::room_lists(cmd.key.unwrap(), &self.db).await
                            }
                            Some(Field::query_create_user) => {
                                models::create_user(cmd.key.unwrap(), &self.db).await
                            }
                            Some(Field::query_user_login) => {
                                models::user_login(cmd.key.unwrap(), &self.db).await
                            }
                            Some(Field::query_user_info) => {
                                models::user_info(cmd.key.unwrap(), &self.db).await
                            }
                            Some(Field::query_create_message) => {
                                models::create_message(cmd.key.unwrap(), &self.db).await
                            }
                            Some(Field::query_message_lists) => {
                                models::message_lists(cmd.key.unwrap(), &self.db).await
                            }
                            _ => unreachable!(),
                        };

                        match res {
                            Ok(res) => {
                                self.conn
                                    .write_buf(Command::response(res, "".to_string()).encode())
                                    .await?
                            }
                            Err(e) => {
                                self.conn
                                    .write_buf(
                                        Command::response(
                                            "".to_string(),
                                            format!("Got error {:?}", e),
                                        )
                                        .encode(),
                                    )
                                    .await?
                            }
                        }

                        info!("FIN QUERY");
                    } else if cmd.is_psync() {
                        info!("PSYNC {:?}", cmd);
                        //master向slave发送的指令
                        //restore
                        //persist
                        let mut cluster = self.db.cluster.write().await;
                        cluster.syncing = true; //宕机恢复开始处理
                        let cmd = cmd.val.unwrap().3;
                        let cmds = serde_json::from_str::<Vec<Command>>(&cmd)?;
                        for cmd in &cmds {
                            cmd.clone().restore(&self.db); //slave收到指令，刷入缓存
                            self.buf_tx.send(cmd.clone()).await?; //slave持久化
                        }
                        cluster.recover = false;
                        cluster.syncing = false; //宕机恢复完毕
                        self.conn.write_buf(Command::psfin().encode()).await?; //通知master，宕机恢复完毕
                        info!("FIN PSYNC");
                    } else if cmd.is_weak() {
                        //接受master发送的弱网标记
                        self.db.cluster.write().await.weaknet = true;
                        self.conn.write_buf(Command::nop().encode()).await?;
                    } else {
                        //这个分支只属于master
                        //如果是master通过自己收到指令，需要直接同步到slave
                        process(Arc::clone(&self.db), cmd).await?;
                    }
                }
                _ => {
                    warn!("failed to read from socket");
                    let peer = self.conn.peer_addr()?;
                    warn!("peer {:?} maybe down", peer);
                    let ipaddr = peer.ip();
                    warn!("{:?}", self.db.cluster.read().await);

                    if self.db.cluster.read().await.slaves.contains_key(&ipaddr) {
                        if let Err(e) = self.failover(ipaddr, SLAVE, false).await {
                            warn!("slave failover error {:?}", e);
                            return Err(e);
                        }

                        warn!("peer {:?} maybe down at slave branch", peer);
                        // return self.try_close().await;
                        return Ok(());
                    }

                    if self.db.cluster.read().await.master == Some(ipaddr) {
                        if let Err(e) = self.failover(ipaddr, MASTER, false).await {
                            warn!("master failover error {:?}", e);
                            return Err(e);
                        }

                        warn!("peer {:?} maybe down at master branch", peer);
                        // return self.try_close().await;
                        return Ok(());
                    }

                    //TODO: 这个错误是怎么产生的
                    warn!("WTF unknown ip {:?}", ipaddr);
                    return Err(format!("WTF unknown ip {:?}", ipaddr).into());
                }
            };
        }

        Ok(())
    }

    #[allow(dead_code)]
    async fn try_close(&mut self) -> Result<()> {
        let (_, rx) = oneshot::channel::<()>();

        tokio::select! {
            res = async {
                self.conn.write_buf(Command::ping().encode()).await?;
                self.conn.read_buf().await
            } => {
                match res? {
                    Some(cmd) if cmd.is_pong() => Ok(()),
                    Some(cmd) => Err(format!("unknown ping response {:?}", cmd).into()),
                    None => Err("peer is dead, shutdown right now".into())
                }
            },
            Err(_) = timeout(Duration::from_secs(300), rx) => Err("ping pong timeout".into()),
        }
    }

    async fn failover(&mut self, ipaddr: IpAddr, kind: &str, fast: bool) -> Result<()> {
        info!("LAGCMD cache started");
        let mut lagcmd = self.db.lagcmd.lock().await;
        if !lagcmd.contains_key(&ipaddr) {
            lagcmd.insert(ipaddr, VecDeque::new());
        }
        drop(lagcmd);
        info!("FIN LAGCMD");

        if kind == SLAVE {
            info!("FAILOVER started as slave branch");
            //从master到slave的连接
            let mut slaves_client = self.db.slaves_client.write().await;
            if let Some(client) = slaves_client.get_mut(&ipaddr) {
                let update;

                if fast {
                    slaves_client.remove(&ipaddr);
                    update = 1;
                    // self.conn.write_buf(Command::offline().encode()).await?;
                } else if !client.ping_pong().await {
                    slaves_client.remove(&ipaddr);
                    update = 1;
                    // self.conn.write_buf(Command::offline().encode()).await?;
                } else {
                    self.conn.write_buf(Command::pass().encode()).await?;
                    update = 2;
                }
                drop(slaves_client);

                let mut cluster = self.db.cluster.write().await;
                if update == 1 {
                    cluster.slaves.remove(&ipaddr);
                    cluster.watching.push(ipaddr);
                    cluster.connected_slaves -= 1;
                } else if update == 2 {
                    cluster.weaknet = false;
                }
                info!("FAILOVER end at slave branch {:?}", cluster);
            } else {
                warn!("slave client {:?} missing", ipaddr);
                self.conn.write_buf(Command::nop().encode()).await?;
            }
        } else {
            info!("FAILOVER started as master branch");
            let mut master_client = self.db.master_client.write().await;
            if let Some(client) = master_client.as_mut() {
                if client.ip != ipaddr {
                    warn!("unknown ip: {:?}", ipaddr);
                    self.conn.write_buf(Command::nop().encode()).await?;
                } else {
                    let update;
                    let mut metas: Vec<(usize, i64, i64, i64, IpAddr)> = vec![];

                    if fast {
                        *master_client = None;
                        update = 1;
                        drop(master_client);

                        let mut slaves_client = self.db.slaves_client.write().await;
                        for slave in slaves_client.values_mut() {
                            metas.push(slave.vote_me().await?);
                        }
                    } else if !client.ping_pong().await {
                        *master_client = None;
                        update = 1;
                        drop(master_client);

                        let mut slaves_client = self.db.slaves_client.write().await;
                        for slave in slaves_client.values_mut() {
                            metas.push(slave.vote_me().await?);
                        }
                    } else {
                        self.conn.write_buf(Command::pass().encode()).await?;
                        update = 2;
                    }

                    let mut cluster = self.db.cluster.write().await;
                    cluster.birthtime = Local::now().timestamp_millis();
                    metas.push((
                        cluster.offset,
                        cluster.last_update,
                        cluster.last_ping,
                        cluster.birthtime,
                        cluster.local_ip.unwrap(),
                    ));
                    metas.sort();
                    metas
                        .iter()
                        .for_each(|v| info!("FAILOVER METAINFO {:?}", v));
                    let new_master_ipaddr = metas.last().unwrap().4;
                    if update == 1 {
                        cluster.master = None;
                        cluster.watching.push(ipaddr);
                        // self.conn.write_buf(Command::offline().encode()).await?;
                        //从slaves中选择一个作为master，并且删除当前的master，更新slaves列表
                        cluster.failover = true;
                        drop(cluster);

                        info!("FAILOVER new master {:?} born", new_master_ipaddr);

                        let mut slaves_client = self.db.slaves_client.write().await;
                        for slave in slaves_client.values_mut() {
                            slave.set_master(new_master_ipaddr.to_string()).await?;
                        }
                    } else if update == 2 {
                        cluster.weaknet = false;
                    }
                    info!("FAILOVER end at master branch");
                }
            } else {
                warn!("master client {:?} missing", ipaddr);
                self.conn.write_buf(Command::nop().encode()).await?;
            }
        }
        info!("FIN FAILOVER");

        Ok(())
    }
}

//TODO: 健康检查频率
#[allow(dead_code)]
struct HealthCheck {
    db: Db,
    shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
}

#[allow(dead_code)]
impl HealthCheck {
    async fn run(&mut self) -> Result<()> {
        let mut health = tokio::time::interval(Duration::from_millis(10000));
        while !self.shutdown.is_shutdown() {
            let res = tokio::select! {
                _ = health.tick() => {
                    // debug!("health checking");
                    self.health_check().await?
                }
                _ = self.shutdown.recv() => {
                    info!("<= exit health checking safely");
                    return Ok(())
                },
            };

            if !res.is_empty() {
                //故障转移
                let mut cluster = self.db.cluster.write().await;
                let mut master_client = self.db.master_client.write().await;
                let mut slaves_client = self.db.slaves_client.write().await;
                for ipaddr in res {
                    if ipaddr == cluster.master.unwrap() {
                        //询问集群内的其他slave，master是否下线，进入主观下线状态，
                        //经过其他slave回应，确实下线了，
                        //slave向其他slave请求设置自己为master，
                        //1. 如果其中有一个slave早于其他slave，则设置最早的这个slave
                        //2. 如果有多个slave遭遇其他slave，则比较这些slave中，offset最大的，last_ping最近的活着last_update最近的
                        //3. 如果仍然有多个slave满足这些条件，则随机从这些slave中选择一个作为master
                        //确定了master，要通知其他slave，某个ip已经是master了，其他的slave要设置成这个ip为master
                        //重新建立了集群，互相发送ping指令，进行试探性通信，与此同时监听当前下线的master，再次上线的时候，加入集群，并设置成slave

                        let mut offline_count = 0;
                        for slave in slaves_client.values_mut() {
                            let cmd = slave.check(ipaddr.to_string(), MASTER.into()).await?;
                            if cmd.is_offline() {
                                offline_count += 1;
                            }
                        }

                        if offline_count > (cluster.connected_slaves - 1) / 2 {
                            *master_client = None;
                            cluster.master = None;
                            cluster.watching.push(ipaddr);
                        }

                        cluster.failover = true;
                        let mut metas: Vec<(usize, i64, i64, i64, IpAddr)> = vec![];
                        for slave in slaves_client.values_mut() {
                            metas.push(slave.vote_me().await?);
                        }

                        cluster.birthtime = Local::now().timestamp_millis();
                        metas.push((
                            cluster.offset,
                            cluster.last_update,
                            cluster.last_ping,
                            cluster.birthtime,
                            cluster.local_ip.unwrap(),
                        ));
                        metas.sort();
                        let new_master_ipaddr = metas.last().unwrap().4;
                        for slave in slaves_client.values_mut() {
                            slave.set_master(new_master_ipaddr.to_string()).await?;
                        }
                    } else {
                        //询问master和集群内其他slave，这个ip是否下线，这个ip进入主观下线状态，
                        //经过集群内的所有机器回应，确实下线了，进入客观下线
                        //同时监听当前下线的ip，当这个ip再次上线时，重新加入集群
                        if let Some(master) = master_client.as_mut() {
                            let cmd = master.check(ipaddr.to_string(), SLAVE.into()).await?;
                            if cmd.is_offline() {
                                slaves_client.remove(&ipaddr);
                                cluster.slaves.remove(&ipaddr);
                                cluster.watching.push(ipaddr);
                                cluster.connected_slaves -= 1;
                            } else if cmd.is_pass() {
                                //TODO: do nothing???
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn health_check(&mut self) -> Result<Vec<IpAddr>> {
        let mut v = vec![];
        if let Some(master) = self.db.master_client.write().await.as_mut() {
            if let Err(e) = master.ping().await {
                warn!("ip: {:?}, err: {:?}", master.ip, e);

                v.push(master.ip);
            }
        }

        for slave in self.db.slaves_client.write().await.values_mut() {
            if let Err(e) = slave.ping().await {
                warn!("ip: {:?}, err: {:?}", slave.ip, e);

                v.push(slave.ip);
            }
        }

        Ok(v)
    }
}

struct BackCheck {
    db: Db,
    shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
}

impl BackCheck {
    async fn run(&mut self) -> Result<()> {
        //每秒check一次，下线的机器是否复活
        let mut watch = tokio::time::interval(Duration::from_secs(1));

        while !self.shutdown.is_shutdown() {
            let _ = tokio::select! {
                _ = watch.tick() => {
                    // debug!("back checking");
                    self.back_check().await
                }
                _ = self.shutdown.recv() => {
                    info!("<= exit back checking safely");
                    return Ok(())
                },
            };
        }
        Ok(())
    }

    async fn back_check(&mut self) -> Result<()> {
        let mut cluster = self.db.cluster.write().await;
        if cluster.watching.is_empty() {
            return Ok(());
        }

        let (_, rx) = oneshot::channel::<()>();

        let res = tokio::select! {
            res = async {
                let ip = cluster.watching.last().unwrap().to_owned();

                let mut client = client::connect(ip).await?;
                if client.ping_pong().await {
                    client.loip(ip.to_string()).await?;

                    if cluster.leader {
                        let ips = cluster
                            .slaves
                            .keys()
                            .filter(|key| **key != ip)
                            .map(|key| key.to_string())
                            .collect::<Vec<String>>()
                            .join(",");
                        client.info(ips).await?;
                        client
                            .set_master(cluster.local_ip.as_ref().unwrap().to_string())
                            .await?;

                        //向client发送psync指令
                        let mut lagcmd = self.db.lagcmd.lock().await;
                        if let Some(lq) = lagcmd.get_mut(&ip) {
                            let cmd = serde_json::to_string(lq)?;
                            let cmd = client.psync(cmd).await?;
                            if cmd.is_psfin() {
                                lq.clear();
                            }
                            lagcmd.remove(&ip);
                        }

                        // if info_reply.is_nop() && master_reply.is_nop() {
                        //     let mut slaves_client = self.db.slaves_client.write().await;
                        //     for slave in slaves_client.values_mut() {
                        //         info!("send {:?} to {:?}", ip, slave.ip);
                        //         slave.info(ip.to_string()).await?;
                        //     }
                        // }
                        self.db.slaves_client.write().await.insert(ip, client);
                        cluster.slaves.insert(ip, ());
                        cluster.connected_slaves += 1;
                    }

                    cluster.watching.pop();
                }
                Result::Ok(())
            } => res?,
            Err(_) = timeout(Duration::from_secs(1), rx) => return Err("back check timeout".into()),
        };

        Ok(res)
    }
}
