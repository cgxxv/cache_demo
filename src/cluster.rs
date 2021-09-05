use std::collections::HashMap;
use std::net::IpAddr;
use std::vec;

use log::warn;
use rand::{seq::SliceRandom, thread_rng};

use crate::client::Client;
use crate::command::Command;
use crate::Result;

#[derive(Debug, Default)]
pub struct Cluster {
    pub master: Option<IpAddr>,
    pub slaves: HashMap<IpAddr, ()>,
    pub local_ip: Option<IpAddr>, //本机ip
    pub watching: Vec<IpAddr>,

    pub connected_slaves: i64,
    pub last_update: i64, //最后一次更新时间戳
    pub last_ping: i64,   //最后一次接受ping的时间
    pub offset: usize,    //数据同步偏移量
    pub birthtime: i64,   //记录产生时间
    pub leader: bool,     //是否是组长，如果是正常情况，组长发送指令同步数据
    pub failover: bool,   //是否处于故障转移阶段
    pub syncing: bool,    //TODO: 标记是否处于恢复阶段的同步状态
    pub recover: bool,    //TODO: 是否处于恢复阶段
    pub weaknet: bool,    //TODO: 处于弱网环境
}

impl Cluster {
    //slave向master发送sync1指令
    pub async fn notify_master(client: &mut Client, cmd: Command) -> Result<Vec<IpAddr>> {
        if let Err(e) = client.sync1(cmd).await {
            warn!("ip: {:?}, err: {:?}", client.ip, e);

            return Ok(vec![client.ip]);
        }

        Ok(vec![])
    }

    //master向slave发送append指令
    pub async fn notify_slave(client: &mut Client, cmd: Command) -> Result<bool> {
        if let Err(e) = client.append(cmd).await {
            warn!("ip: {:?}, err: {:?}", client.ip, e);

            return Ok(false);
        }

        Ok(true)
    }

    pub async fn random_slaveip(&self) -> Option<IpAddr> {
        let mut rng = thread_rng();
        let res = self
            .slaves
            .keys()
            .clone()
            .map(|ipaddr| *ipaddr)
            .collect::<Vec<IpAddr>>();
        let res = res.choose(&mut rng);
        if let Some(res) = res {
            return Some(*res);
        }

        None
    }
}
