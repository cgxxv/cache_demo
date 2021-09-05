use std::net::IpAddr;

use chrono::prelude::Local;
use log::{debug, info, warn};
use pnet_datalink::interfaces;

use crate::client;
use crate::models::Db;

pub async fn update_cluster(
    mut ips: Vec<IpAddr>,
    db: Db,
) -> Result<impl warp::Reply, warp::Rejection> {
    let mut cluster = db.cluster.write().await;
    //默认标记recover=true，为了同步处理宕机恢复问题
    cluster.recover = true;
    interfaces().iter().for_each(|iface| {
        iface.ips.iter().filter(|ip| ip.is_ipv4()).for_each(|ipv4| {
            info!("{:?}", ipv4);
            if ips.contains(&ipv4.ip()) {
                cluster.local_ip = Some(ipv4.ip());
            }
        })
    });

    let master_ip = ips.pop();

    let mut slaves_client = db.slaves_client.write().await;
    if cluster.local_ip == master_ip {
        cluster.leader = true;
        cluster.birthtime = Local::now().timestamp_millis();

        for ip in &ips {
            match client::connect(*ip).await {
                Ok(client) => {
                    slaves_client.insert(*ip, client);
                }
                Err(e) => {
                    warn!("err: {:?}, {:?}", e, ip);
                }
            }
            cluster.connected_slaves += 1;
        }
    } else {
        for ip in &ips {
            if Some(*ip) != cluster.local_ip {
                match client::connect(*ip).await {
                    Ok(client) => {
                        slaves_client.insert(*ip, client);
                    }
                    Err(e) => {
                        warn!("err: {:?}, {:?}", e, ip);
                    }
                }
                cluster.connected_slaves += 1;
            }
        }

        let mut master_client = db.master_client.write().await;
        match client::connect(master_ip.unwrap()).await {
            Ok(client) => {
                *master_client = Some(client);
            }
            Err(e) => {
                warn!("err: {:?}, {:?}", e, &master_ip);
            }
        }
        drop(master_client);
    }
    drop(slaves_client);
    cluster.master = master_ip;
    ips.iter().for_each(|ipaddr| {
        cluster.slaves.insert(*ipaddr, ());
        ()
    });
    debug!("{:#?}", cluster);
    // debug!("{:#?}", db.master_client);
    // debug!("{:#?}", db.slaves_client);

    Ok("")
}

pub async fn check_cluster(db: Db) -> Result<impl warp::Reply, warp::Rejection> {
    let mut cluster = db.cluster.write().await;
    debug!("check cluster");
    if let Some(master) = db.master_client.write().await.as_mut() {
        match master.ping().await {
            Ok(cmd) => assert!(cmd.is_pong()),
            Err(e) => {
                warn!("ip: {:?}, err: {:?}", master.ip, e);
            }
        }
    }

    for slave in db.slaves_client.write().await.values_mut() {
        match slave.ping().await {
            Ok(cmd) => assert!(cmd.is_pong()),
            Err(e) => {
                warn!("ip: {:?}, err: {:?}", slave.ip, e);
            }
        }
    }
    cluster.recover = false;

    Ok("")
}
