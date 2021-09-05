use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::io::{prelude::*, BufReader};
use std::net::IpAddr;
use std::path::Path;
use std::sync::Arc;

use chrono::prelude::Local;
use dashmap::DashMap;
use fxhash::FxBuildHasher;
use log::{debug, info};
use parking_lot::RwLock;
use reqwest::Client;
use serde::{self, Deserialize, Serialize};
use sha1::Sha1;
use tokio::sync::{self, mpsc};
use uuid::Uuid;

use crate::cluster::Cluster;
use crate::command::{Command, Field};
use crate::{Error::WhatError, Result, BUFSIZE, HTTP_PORT};

pub const CHAT_DB_FILE: &str = "chat.db";

pub type Db = Arc<Handler>;

#[derive(Debug)]
pub struct Handler {
    pub cache: Cache,
    pub cmd_tx: mpsc::Sender<Command>,
    pub cluster: sync::RwLock<Cluster>,
    // TODO: 增加连接池
    pub master_client: sync::RwLock<Option<crate::client::Client>>, //socket连接，主
    // TODO: 增加连接池
    pub slaves_client: sync::RwLock<HashMap<IpAddr, crate::client::Client>>, //socket连接，从
    pub lagcmd: sync::Mutex<HashMap<IpAddr, VecDeque<Command>>>,             //TODO: 延迟同步的指令
}

#[derive(Debug, Default)]
pub struct Cache {
    pub user_dict: DashMap<String, String, FxBuildHasher>, //username => user_info
    pub user_secret: DashMap<String, String, FxBuildHasher>, //username => password
    pub user_token: DashMap<String, String, FxBuildHasher>, //token => username

    pub room_name_dict: DashMap<String, String, FxBuildHasher>, //room_name => roomid
    pub room_dict: DashMap<String, String, FxBuildHasher>,      //roomid => room_name
    pub roomid_list: RwLock<Vec<String>>,
    pub room_user: DashMap<String, HashSet<String, FxBuildHasher>, FxBuildHasher>, //roomid => user_list
    pub user_in_room: DashMap<String, String, FxBuildHasher>,                      //token => roomid

    pub room_message_dict: DashMap<String, DashMap<String, String, FxBuildHasher>, FxBuildHasher>, //roomid => message_dict
    pub room_messageid_list: DashMap<String, Vec<String>, FxBuildHasher>, //roomid => messageid_list
}

pub fn init_db() -> Result<(Db, mpsc::Receiver<Command>)> {
    let (cmd_tx, cmd_rx) = mpsc::channel(BUFSIZE);

    let handler = Handler {
        cache: Cache::default(),
        cmd_tx,
        cluster: sync::RwLock::new(Cluster::default()),
        master_client: sync::RwLock::new(None),
        slaves_client: sync::RwLock::new(HashMap::new()),
        lagcmd: sync::Mutex::new(HashMap::new()),
    };
    let db = Arc::new(handler);
    if Path::new(CHAT_DB_FILE).exists() {
        let file = File::open(CHAT_DB_FILE)?;
        let reader = BufReader::new(file);
        for line in reader.lines() {
            // debug!("{:?}", &line);
            let cmd = Command::decode(line?.as_str())?;
            // debug!("{:?}", &cmd);
            cmd.restore(&db);
        }
    }

    Ok((db, cmd_rx))
}

static LIM_LENGTH: usize = 200;
static SPE_LENGTH: usize = 36;

#[derive(Debug, Serialize, Deserialize)]
pub struct Room {
    id: String,
    name: String,
}

impl Room {
    pub async fn create(db: &Db, name: &str) -> Result<String> {
        if name.len() > LIM_LENGTH || name.is_empty() {
            return Err(WhatError);
        }

        let cluster = db.cluster.read().await;
        info!("CLUSTER {:?}", cluster);
        if cluster.recover || cluster.syncing || cluster.weaknet {
            if let Some(ipaddr) = cluster.master.as_ref() {
                let resp = Client::new()
                    .post(format!("http://{}:{}/room", ipaddr.to_string(), HTTP_PORT))
                    .json(&serde_json::json!({
                        "name": name,
                    }))
                    .send()
                    .await?
                    .text()
                    .await?;
                debug!("{:#?}", resp);
                return Ok(resp);
            } else {
                return Err(WhatError);
            }
        }

        if !cluster.leader {
            let mut client = db.master_client.write().await;
            let client = client.as_mut().unwrap();
            let cmd = client
                .write(Command::query(Field::query_create_room, name.to_string()))
                .await?;

            let errinfo = cmd.val.unwrap().3;
            if errinfo.is_empty() {
                return Ok(cmd.key.unwrap());
            }
            return Err(errinfo.into());
        }
        drop(cluster);

        Ok(create_room(name, db).await?)
    }

    pub async fn enter(db: &Db, roomid: &str, token: &str) -> Result<()> {
        if roomid.len() != SPE_LENGTH
            || token.len() != SPE_LENGTH
            || !db.cache.room_dict.contains_key(roomid)
        {
            return Err(WhatError);
        }

        let cluster = db.cluster.read().await;
        info!("CLUSTER {:?}", cluster);
        if cluster.recover || cluster.syncing || cluster.weaknet {
            if let Some(ipaddr) = cluster.master.as_ref() {
                let resp = Client::new()
                    .put(format!(
                        "http://{}:{}/room/{}/enter",
                        ipaddr.to_string(),
                        HTTP_PORT,
                        roomid
                    ))
                    .header("Authorization", format!("Bearer {}", token))
                    .send()
                    .await?;
                debug!("{:#?}", resp);
                return Ok(());
            } else {
                return Err(WhatError);
            }
        }

        let key = format!("{},{}", roomid, token);
        if !cluster.leader {
            let mut client = db.master_client.write().await;
            let client = client.as_mut().unwrap();
            let cmd = client
                .write(Command::query(Field::query_enter_room, key.clone()))
                .await?;

            let errinfo = cmd.val.unwrap().3;
            if errinfo.is_empty() {
                return Ok(());
            }
            return Err(errinfo.into());
        }
        drop(cluster);

        match enter_room(key, db).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn leave(db: &Db, token: &str) -> Result<()> {
        if token.len() != SPE_LENGTH {
            return Err(WhatError);
        }

        let cluster = db.cluster.read().await;
        info!("CLUSTER {:?}", cluster);
        if cluster.recover || cluster.syncing || cluster.weaknet {
            if let Some(ipaddr) = cluster.master.as_ref() {
                let resp = Client::new()
                    .put(format!(
                        "http://{}:{}/roomLeave",
                        ipaddr.to_string(),
                        HTTP_PORT
                    ))
                    .header("Authorization", format!("Bearer {}", token))
                    .send()
                    .await?;
                debug!("{:#?}", resp);
                return Ok(());
            } else {
                return Err(WhatError);
            }
        }

        if !cluster.leader {
            let mut client = db.master_client.write().await;
            let client = client.as_mut().unwrap();
            let cmd = client
                .write(Command::query(Field::query_leave_room, token.to_string()))
                .await?;

            let errinfo = cmd.val.unwrap().3;
            if errinfo.is_empty() {
                return Ok(());
            }
            return Err(errinfo.into());
        }
        drop(cluster);

        match leave_room(token, db).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn get_roominfo(db: Db, roomid: String) -> Result<String> {
        if roomid.len() != SPE_LENGTH {
            return Err(WhatError);
        }

        let cluster = db.cluster.read().await;
        info!("CLUSTER {:?}", cluster);
        if cluster.recover || cluster.syncing || cluster.weaknet {
            if let Some(ipaddr) = cluster.random_slaveip().await {
                let resp = Client::new()
                    .get(format!(
                        "http://{}:{}/room/{}",
                        ipaddr.to_string(),
                        HTTP_PORT,
                        roomid,
                    ))
                    .send()
                    .await?
                    .text()
                    .await?;
                debug!("{:#?}", resp);
                return Ok(resp);
            } else {
                return Err(WhatError);
            }
        }

        if !cluster.leader {
            let mut client = db.master_client.write().await;
            let client = client.as_mut().unwrap();
            let cmd = client
                .write(Command::query(Field::query_room_info, roomid.clone()))
                .await?;

            let errinfo = cmd.val.unwrap().3;
            if errinfo.is_empty() {
                return Ok(cmd.key.unwrap());
            }
            return Err(errinfo.into());
        }
        drop(cluster);

        Ok(room_info(roomid, &db).await?)
    }

    pub async fn get_users(db: Db, roomid: String) -> Result<String> {
        if roomid.len() != SPE_LENGTH {
            return Err(WhatError);
        }

        let cluster = db.cluster.read().await;
        info!("CLUSTER {:?}", cluster);
        if cluster.recover || cluster.syncing || cluster.weaknet {
            if let Some(ipaddr) = cluster.random_slaveip().await {
                let resp = Client::new()
                    .get(format!(
                        "http://{}:{}/room/{}/users",
                        ipaddr.to_string(),
                        HTTP_PORT,
                        roomid
                    ))
                    .send()
                    .await?
                    .text()
                    .await?;
                debug!("{:#?}", resp);
                return Ok(resp);
            } else {
                return Err(WhatError);
            }
        }

        if !cluster.leader {
            let mut client = db.master_client.write().await;
            let client = client.as_mut().unwrap();
            let cmd = client
                .write(Command::query(Field::query_room_users, roomid.clone()))
                .await?;

            let errinfo = cmd.val.unwrap().3;
            if errinfo.is_empty() {
                return Ok(cmd.key.unwrap());
            }
            return Err(errinfo.into());
        }
        drop(cluster);

        Ok(room_users(roomid, &db).await?)
    }

    pub async fn query(db: Db, index: isize, size: isize) -> Result<String> {
        let cluster = db.cluster.read().await;
        info!("CLUSTER {:?}", cluster);
        if cluster.recover || cluster.syncing || cluster.weaknet {
            if let Some(ipaddr) = cluster.random_slaveip().await {
                let resp = Client::new()
                    .post(format!(
                        "http://{}:{}/roomList",
                        ipaddr.to_string(),
                        HTTP_PORT
                    ))
                    .json(&serde_json::json!({
                        "pageIndex": index,
                        "pageSize": size,
                    }))
                    .send()
                    .await?
                    .text()
                    .await?;
                debug!("{:#?}", resp);
                return Ok(resp);
            } else {
                return Err(WhatError);
            }
        }

        let key = format!("{},{}", index, size);
        if !cluster.leader {
            let mut client = db.master_client.write().await;
            let client = client.as_mut().unwrap();
            let cmd = client
                .write(Command::query(Field::query_room_lists, key.clone()))
                .await?;

            let errinfo = cmd.val.unwrap().3;
            if errinfo.is_empty() {
                return Ok(cmd.key.unwrap());
            }
            return Err(errinfo.into());
        }
        drop(cluster);

        Ok(room_lists(key, &db).await?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct User {
    #[serde(skip_serializing)]
    pub username: String,
    #[serde(rename(serialize = "firstName", deserialize = "firstName"))]
    pub first_name: String,
    #[serde(rename(serialize = "lastName", deserialize = "lastName"))]
    pub last_name: String,
    pub email: String,
    #[serde(skip_serializing)]
    pub password: Option<String>,
    pub phone: String,
}

impl User {
    pub async fn create(&mut self, db: &Db) -> Result<()> {
        if self.username.len() > LIM_LENGTH
            || self.username.is_empty()
            || self.first_name.len() > LIM_LENGTH
            || self.first_name.is_empty()
            || self.last_name.len() > LIM_LENGTH
            || self.last_name.is_empty()
            || self.email.len() > LIM_LENGTH
            || self.email.is_empty()
            || self.phone.len() > LIM_LENGTH
            || self.phone.is_empty()
        {
            return Err(WhatError);
        }

        let cluster = db.cluster.read().await;
        info!("CLUSTER {:?}", cluster);
        if cluster.recover || cluster.syncing || cluster.weaknet {
            if let Some(ipaddr) = cluster.master.as_ref() {
                let resp = Client::new()
                    .post(format!("http://{}:{}/user", ipaddr.to_string(), HTTP_PORT))
                    .json(self)
                    .send()
                    .await?;
                debug!("{:#?}", resp);
                return Ok(());
            } else {
                return Err(WhatError);
            }
        }

        let key = format!(
            "{}\t{}\t{}\t{}\t{}\t{}",
            self.username,
            self.first_name,
            self.last_name,
            self.email,
            self.password.as_ref().unwrap(),
            self.phone
        );
        if !cluster.leader {
            let mut client = db.master_client.write().await;
            let client = client.as_mut().unwrap();
            let cmd = client
                .write(Command::query(Field::query_create_user, key.clone()))
                .await?;

            let errinfo = cmd.val.unwrap().3;
            if errinfo.is_empty() {
                return Ok(());
            }
            return Err(errinfo.into());
        }
        drop(cluster);

        match create_user(key, db).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn login(db: &Db, username: &str, password: &str) -> Result<String> {
        if username.len() > LIM_LENGTH || username.is_empty() || password.len() > LIM_LENGTH {
            return Err(WhatError);
        }

        let cluster = db.cluster.read().await;
        info!("CLUSTER {:?}", cluster);
        if cluster.recover || cluster.syncing || cluster.weaknet {
            if let Some(ipaddr) = cluster.master.as_ref() {
                let resp = Client::new()
                    .get(format!(
                        "http://{}:{}/userLogin",
                        ipaddr.to_string(),
                        HTTP_PORT
                    ))
                    .form(&[("username", username), ("password", password)])
                    .send()
                    .await?
                    .text()
                    .await?;
                debug!("{:#?}", resp);
                return Ok(resp);
            } else {
                return Err(WhatError);
            }
        }

        let key = format!("{}\t{}", username, password);
        if !cluster.leader {
            let mut client = db.master_client.write().await;
            let client = client.as_mut().unwrap();
            let cmd = client
                .write(Command::query(Field::query_user_login, key.clone()))
                .await?;

            let errinfo = cmd.val.unwrap().3;
            if errinfo.is_empty() {
                return Ok(cmd.key.unwrap());
            }
            return Err(errinfo.into());
        }
        drop(cluster);

        Ok(user_login(key, db).await?)
    }

    pub async fn get_userinfo(db: Db, username: String) -> Result<String> {
        if username.len() > LIM_LENGTH || username.is_empty() {
            return Err(WhatError);
        }

        let cluster = db.cluster.read().await;
        info!("CLUSTER {:?}", cluster);
        if cluster.recover || cluster.syncing || cluster.weaknet {
            if let Some(ipaddr) = cluster.random_slaveip().await {
                let resp = Client::new()
                    .get(format!(
                        "http://{}:{}/user/{}",
                        ipaddr.to_string(),
                        HTTP_PORT,
                        username
                    ))
                    .send()
                    .await?
                    .text()
                    .await?;
                debug!("{:#?}", resp);
                return Ok(resp);
            } else {
                return Err(WhatError);
            }
        }

        if !cluster.leader {
            let mut client = db.master_client.write().await;
            let client = client.as_mut().unwrap();
            let cmd = client
                .write(Command::query(Field::query_user_info, username.clone()))
                .await?;

            let errinfo = cmd.val.unwrap().3;
            if errinfo.is_empty() {
                return Ok(cmd.key.unwrap());
            }
            return Err(errinfo.into());
        }
        drop(cluster);

        Ok(user_info(username, &db).await?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    pub id: String,
    pub text: String,
    pub timestamp: Option<String>,
}

impl Message {
    pub async fn create(&mut self, db: &Db, token: &str) -> Result<()> {
        if token.len() != SPE_LENGTH
            || self.id.len() > LIM_LENGTH
            || self.id.is_empty()
            || self.text.len() > LIM_LENGTH
            || self.text.is_empty()
        {
            return Err(WhatError);
        }

        let cluster = db.cluster.read().await;
        info!("CLUSTER {:?}", cluster);
        if cluster.recover || cluster.syncing || cluster.weaknet {
            if let Some(ipaddr) = cluster.master.as_ref() {
                let resp = Client::new()
                    .post(format!(
                        "http://{}:{}/message/send",
                        ipaddr.to_string(),
                        HTTP_PORT
                    ))
                    .header("Authorization", format!("Bearer {}", token))
                    .json(self)
                    .send()
                    .await?;
                debug!("{:#?}", resp);
                return Ok(());
            } else {
                return Err(WhatError);
            }
        }

        let key = format!("{}\t{}\t{}", self.id, self.text, token);
        if !cluster.leader {
            let mut client = db.master_client.write().await;
            let client = client.as_mut().unwrap();
            let cmd = client
                .write(Command::query(Field::query_create_message, key.clone()))
                .await?;

            let errinfo = cmd.val.unwrap().3;
            if errinfo.is_empty() {
                return Ok(());
            }
            return Err(errinfo.into());
        }
        drop(cluster);

        match create_message(key, db).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn query(db: Db, index: isize, size: isize, token: String) -> Result<String> {
        if token.len() != SPE_LENGTH {
            return Err(WhatError);
        }

        let cluster = db.cluster.read().await;
        info!("CLUSTER {:?}", cluster);
        if cluster.recover || cluster.syncing || cluster.weaknet {
            if let Some(ipaddr) = cluster.random_slaveip().await {
                let resp = Client::new()
                    .post(format!(
                        "http://{}:{}/message/retrieve",
                        ipaddr.to_string(),
                        HTTP_PORT
                    ))
                    .header("Authorization", format!("Bearer {}", token))
                    .json(&serde_json::json!({
                        "pageIndex": index,
                        "pageSize": size,
                    }))
                    .send()
                    .await?
                    .text()
                    .await?;
                debug!("{:#?}", resp);
                return Ok(resp);
            } else {
                return Err(WhatError);
            }
        }

        let key = format!("{},{},{}", index, size, token);
        if !cluster.leader {
            let mut client = db.master_client.write().await;
            let client = client.as_mut().unwrap();
            let cmd = client
                .write(Command::query(Field::query_message_lists, key.clone()))
                .await?;

            let errinfo = cmd.val.unwrap().3;
            if errinfo.is_empty() {
                return Ok(cmd.key.unwrap());
            }
            return Err(errinfo.into());
        }
        drop(cluster);

        Ok(message_lists(key, &db).await?)
    }
}

pub async fn create_room(name: &str, db: &Db) -> Result<String> {
    if let Some(roomid) = db.cache.room_name_dict.get(name) {
        return Ok(roomid.to_string());
    }

    let id = Uuid::new_v4().to_string();
    let roomid_list = Command::roomid_list(id.clone());
    let room_dict = Command::room_dict(id.clone(), name.to_string());
    let room_name_dict = Command::room_name_dict(name.to_string(), id.clone());
    let init_room_user = Command::init_room_user(id.clone());

    // if !db.cluster.read().await.leader{
    //     let mut client = db.master_client.write().await;
    //     let client = client.as_mut().unwrap();
    //     client.write(Command::sync1(roomid_list)).await?;
    //     client.write(Command::sync1(room_dict)).await?;
    //     client.write(Command::sync1(room_name_dict)).await?;
    //     client.write(Command::sync1(init_room_user)).await?;

    //     return Ok(id)
    // }

    db.cache.roomid_list.write().push(id.clone());
    db.cache.room_dict.insert(id.clone(), name.to_string());
    db.cache.room_name_dict.insert(name.to_string(), id.clone());
    db.cache
        .room_user
        .insert(id.clone(), HashSet::with_hasher(FxBuildHasher::default()));

    db.cmd_tx.send(roomid_list).await?;
    db.cmd_tx.send(room_dict).await?;
    db.cmd_tx.send(room_name_dict).await?;
    db.cmd_tx.send(init_room_user).await?;

    Ok(id)
}

// key => roomid,token
pub async fn enter_room(key: String, db: &Db) -> Result<String> {
    let v: Vec<&str> = key.split(',').collect();
    let roomid = v[0];
    let token = v[1];

    let username = db
        .cache
        .user_token
        .get(token)
        .map(|username| username.to_owned())
        .ok_or(WhatError)?;

    // let srem_room_user = Command::srem_room_user(roomid.to_string(), username.to_string());
    // let mut srem_room_user = None;
    let sadd_room_user = Command::sadd_room_user(roomid.to_string(), username.to_string());
    let add_user_in_room = Command::add_user_in_room(token.to_string(), roomid.to_string());

    if let Some(roomid) = db
        .cache
        .user_in_room
        .get(token)
        .map(|roomid| roomid.to_owned())
    {
        if let Some(mut userset) = db.cache.room_user.get_mut(&roomid) {
            userset.remove(&username);
            let srem_room_user = Some(Command::srem_room_user(
                roomid.to_string(),
                username.to_string(),
            ));
            db.cmd_tx.send(srem_room_user.clone().unwrap()).await?;
        }
    }

    // if !db.cluster.read().await.leader{
    //     let mut client = db.master_client.write().await;
    //     let client = client.as_mut().unwrap();
    //     if let Some(srem_room_user) = srem_room_user {
    //         client.write(Command::sync1(srem_room_user)).await?;
    //     }
    //     client.write(Command::sync1(sadd_room_user)).await?;
    //     client.write(Command::sync1(add_user_in_room)).await?;

    //     return Ok("".to_string())
    // }

    if let Some(mut userset) = db.cache.room_user.get_mut(roomid) {
        userset.insert(username);
    }
    db.cache
        .user_in_room
        .insert(token.to_string(), roomid.to_string());

    db.cmd_tx.send(sadd_room_user.clone()).await?;
    db.cmd_tx.send(add_user_in_room.clone()).await?;

    Ok("".to_string())
}

pub async fn leave_room(token: &str, db: &Db) -> Result<String> {
    let username = db
        .cache
        .user_token
        .get(token)
        .map(|username| username.to_owned())
        .ok_or(WhatError)?;

    let roomid = db
        .cache
        .user_in_room
        .get(token)
        .map(|roomid| roomid.to_owned())
        .ok_or(WhatError)?;

    let srem_room_user = Command::srem_room_user(roomid.clone(), username.clone());
    let rem_user_in_room = Command::rem_user_in_room(token.to_string());

    if let Some(mut userset) = db.cache.room_user.get_mut(&roomid) {
        userset.remove(&username);
    }
    db.cache.user_in_room.remove(token);

    db.cmd_tx.send(srem_room_user.clone()).await?;
    db.cmd_tx.send(rem_user_in_room.clone()).await?;

    // let mut client = db.master_client.write().await;
    // let client = client.as_mut().unwrap();
    // client.write(Command::sync1(srem_room_user)).await?;
    // client.write(Command::sync1(rem_user_in_room)).await?;

    Ok("".to_string())
}

// key => roomid
pub async fn room_info(key: String, db: &Db) -> Result<String> {
    Ok(db
        .cache
        .room_dict
        .get(&key)
        .map(|name| name.to_owned())
        .ok_or(WhatError)?)
}

// key => roomid
pub async fn room_users(key: String, db: &Db) -> Result<String> {
    let res: Vec<String> = db
        .cache
        .room_user
        .get(&key)
        .map(|userset| userset.iter().cloned().collect())
        .ok_or(WhatError)?;
    Ok(serde_json::to_string(&res)?)
}

// key => index,size
pub async fn room_lists(key: String, db: &Db) -> Result<String> {
    let v: Vec<&str> = key.split(',').collect();
    let index: isize = v[0].parse()?;
    let mut size: isize = v[1].parse()?;

    let roomid_list = db.cache.roomid_list.read();
    let length = roomid_list.len() as isize;

    if index >= length {
        return Ok("".to_string());
    }
    if size > length {
        size = length;
    }

    let res: Vec<Room> = roomid_list[index as usize..size as usize]
        .iter()
        .map(|roomid| -> Result<Room> {
            let name = db
                .cache
                .room_dict
                .get(roomid)
                .map(|name| name.to_owned())
                .ok_or(WhatError)?;
            Ok(Room {
                id: roomid.to_owned(),
                name,
            })
        })
        .filter_map(|room| room.ok())
        .collect();

    Ok(serde_json::to_string(&res)?)
}

// key => username\tfirst_name\tlast_name\temail\tpassword\tphone
pub async fn create_user(key: String, db: &Db) -> Result<String> {
    let v: Vec<&str> = key.split('\t').collect();
    let username = v[0];
    let first_name = v[1];
    let last_name = v[2];
    let email = v[3];
    let password = v[4];
    let phone = v[5];

    if db.cache.user_dict.contains_key(username) {
        return Err(WhatError);
    }

    let mut hasher = Sha1::new();
    hasher.update(password.as_bytes());
    let password = hasher.hexdigest();

    let user = User {
        username: username.to_string(),
        first_name: first_name.to_string(),
        last_name: last_name.to_string(),
        email: email.to_string(),
        password: None,
        phone: phone.to_string(),
    };
    let userinfo = serde_json::to_string(&user).map_err(|_| WhatError)?;

    let user_dict = Command::user_dict(username.to_owned(), userinfo.clone());
    let user_secret = Command::user_secret(username.to_owned(), password.clone());

    db.cache.user_dict.insert(username.to_owned(), userinfo);
    db.cache.user_secret.insert(username.to_owned(), password);

    db.cmd_tx.send(user_dict.clone()).await?;
    db.cmd_tx.send(user_secret.clone()).await?;

    // let mut client = db.master_client.write().await;
    // let client = client.as_mut().unwrap();
    // client.write(Command::sync1(user_dict)).await?;
    // client.write(Command::sync1(user_secret)).await?;

    Ok("".to_string())
}

// key => username\tpassword
pub async fn user_login(key: String, db: &Db) -> Result<String> {
    let v: Vec<&str> = key.split('\t').collect();
    let username = v[0];
    let password = v[1];

    let passcache = db
        .cache
        .user_secret
        .get(username)
        .map(|passcache| passcache.to_owned())
        .ok_or(WhatError)?;

    let mut hasher = Sha1::new();
    hasher.update(password.as_bytes());

    if passcache != hasher.hexdigest() {
        return Ok("".to_string());
    }

    let token = Uuid::new_v4().to_string();
    let user_token = Command::user_token(token.clone(), username.to_owned());
    db.cache
        .user_token
        .insert(token.clone(), username.to_owned());

    db.cmd_tx.send(user_token.clone()).await?;

    // let mut client = db.master_client.write().await;
    // let client = client.as_mut().unwrap();
    // client.write(Command::sync1(user_token)).await?;

    Ok(token)
}

// key => username
pub async fn user_info(key: String, db: &Db) -> Result<String> {
    Ok(db
        .cache
        .user_dict
        .get(&key)
        .map(|userinfo| userinfo.to_owned())
        .ok_or(WhatError)?)
}

// key => id\ttext\ttoken
pub async fn create_message(key: String, db: &Db) -> Result<String> {
    let v: Vec<&str> = key.split('\t').collect();
    let id = v[0];
    let text = v[1];
    let token = v[2];

    let roomid = db
        .cache
        .user_in_room
        .get(token)
        .map(|roomid| roomid.to_owned())
        .ok_or(WhatError)?;

    if let Some(message_dict) = db.cache.room_message_dict.get(&roomid) {
        if message_dict.contains_key(id) {
            return Err(WhatError);
        }
    }

    let msg = Message {
        id: id.to_string(),
        text: text.to_string(),
        timestamp: Some(Local::now().timestamp().to_string()),
    };

    let val = serde_json::to_string(&msg).map_err(|_| WhatError)?;

    let room_messageid_list = Command::room_messageid_list(roomid.clone(), id.to_string());
    let room_message_dict =
        Command::room_message_dict(roomid.clone(), (id.to_string(), val.clone()));

    let messageid_list = db.cache.room_messageid_list.get_mut(&roomid);
    if let Some(mut messageid_list) = messageid_list {
        messageid_list.push(id.to_string());
    } else {
        db.cache
            .room_messageid_list
            .insert(roomid.clone(), vec![id.to_string()]);
    }
    db.cmd_tx.send(room_messageid_list.clone()).await?;

    if let Some(message_dict) = db.cache.room_message_dict.get_mut(&roomid) {
        message_dict.insert(id.to_owned(), val);
    } else {
        let message_dict = DashMap::with_hasher(FxBuildHasher::default());
        message_dict.insert(id.to_owned(), val);
        db.cache.room_message_dict.insert(roomid, message_dict);
    }
    db.cmd_tx.send(room_message_dict.clone()).await?;

    // let mut client = db.master_client.write().await;
    // let client = client.as_mut().unwrap();
    // client.write(Command::sync1(room_messageid_list)).await?;
    // client.write(Command::sync1(room_message_dict)).await?;

    Ok("".to_string())
}

// key => index,size,token
pub async fn message_lists(key: String, db: &Db) -> Result<String> {
    let v: Vec<&str> = key.split(',').collect();
    let index: isize = v[0].to_string().parse()?;
    let size: isize = v[1].to_string().parse()?;
    let token = v[2];

    let roomid = db
        .cache
        .user_in_room
        .get(token)
        .map(|roomid| roomid.to_owned())
        .ok_or(WhatError)?;

    let message_dict = db.cache.room_message_dict.get(&roomid).ok_or(WhatError)?;
    let messageids = db.cache.room_messageid_list.get(&roomid).ok_or(WhatError)?;

    info!("QUERY index = {}, size = {}", index, size);
    let llen = messageids.len() as isize;
    // let mut start = llen + index;
    // let mut end = size + 1;
    let mut end = llen + index + 1;
    let mut start = end - size + 1;

    // stop += 1;
    if end > llen {
        end = llen;
    }
    if end < 0 {
        end = 0;
    }
    if start < 0 {
        start = 0;
    }
    info!("QUERY1 start = {}, end = {}, llen = {}", start, end, llen);
    if start > end || start >= llen {
        return Ok("".to_string());
    }
    // if end >= llen {
    //     end = llen;
    // }
    info!(
        "QUERY2 start = {}, end = {}, llen = {}",
        start as usize, end as usize, llen
    );
    info!(
        "QUEYR3 messageids = {:?}",
        &messageids[start as usize..end as usize]
    );

    let res: Vec<Message> = messageids[start as usize..end as usize]
        .iter()
        .rev()
        .map(|messageid| -> Result<Message> {
            let msginfo = message_dict.get(messageid).ok_or(WhatError)?;
            info!(
                "QUEYR4 messageid = {}, msginfo.key = {:?}, msginfo.value = {:?}",
                messageid,
                msginfo.key(),
                msginfo.value()
            );
            let message: Message = serde_json::from_str(msginfo.value()).map_err(|_| WhatError)?;

            Ok(message)
        })
        .filter_map(|message| message.ok())
        .collect();

    Ok(serde_json::to_string(&res)?)
}
