#![allow(clippy::upper_case_acronyms)]
use std::collections::HashSet;
use std::io::Cursor;
use std::str::{self, FromStr};

use dashmap::DashMap;
use fxhash::FxBuildHasher;
use log::debug;
use serde::{Deserialize, Serialize};

use crate::models::Db;

static NONE: &str = "NONE";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Action {
    INIT,
    HSET,
    ADD,
    REM,
    PUSH,

    /* service command start */
    QUIT,
    PING,
    PONG,
    CHECK,
    PASS,
    OFFLINE,
    VOTE,
    META, //查询meta信息， last_update, laster_ping, offset, birthtime
    MASTER,
    APPEND, //用于持久化
    SYNC1,  //同步单独的指令
    QUERY,  //查询指令
    RESPONSE,  //响应
    PSYNC,
    // GOON,
    PSFIN,
    INFO,
    LOIP, //用于更新自己的ip
    WEAK, //弱网环境
    NULL,
    /* service command end */
}

impl FromStr for Action {
    type Err = ();

    fn from_str(input: &str) -> Result<Action, Self::Err> {
        match input {
            "INIT" => Ok(Action::INIT),
            "HSET" => Ok(Action::HSET),
            "ADD" => Ok(Action::ADD),
            "REM" => Ok(Action::REM),
            "PUSH" => Ok(Action::PUSH),

            /* service command start */
            "QUIT" => Ok(Action::QUIT),
            "PING" => Ok(Action::PING),
            "PONG" => Ok(Action::PONG),
            "CHECK" => Ok(Action::CHECK),
            "PASS" => Ok(Action::PASS),
            "OFFLINE" => Ok(Action::OFFLINE),
            "VOTE" => Ok(Action::VOTE),
            "META" => Ok(Action::META), //元信息查询
            "MASTER" => Ok(Action::MASTER),
            "APPEND" => Ok(Action::APPEND), //用于持久化
            "SYNC1" => Ok(Action::SYNC1),   //同步单独的指令
            "QUERY" => Ok(Action::QUERY),   //查询指令
            "RESPONSE" => Ok(Action::RESPONSE),
            "PSYNC" => Ok(Action::PSYNC),
            "PSFIN" => Ok(Action::PSFIN),
            "INFO" => Ok(Action::INFO),
            "LOIP" => Ok(Action::LOIP), //用于更新自己的ip
            "WEAK" => Ok(Action::WEAK), //弱网环境
            "NULL" => Ok(Action::NULL),
            /* service command end */
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(non_camel_case_types)]
pub enum Field {
    user_dict,
    user_secret,
    user_token,

    room_name_dict,
    room_dict,
    roomid_list,
    room_user,
    user_in_room,

    room_message_dict,
    room_messageid_list,


    query_create_room,
    query_enter_room,
    query_leave_room,
    query_room_users,
    query_room_info,
    query_room_lists,
    query_create_user,
    query_user_login,
    query_user_info,
    query_create_message,
    query_message_lists,

    null,
}

impl FromStr for Field {
    type Err = ();

    fn from_str(input: &str) -> Result<Field, Self::Err> {
        match input {
            "user_dict" => Ok(Field::user_dict),
            "user_secret" => Ok(Field::user_secret),
            "user_token" => Ok(Field::user_token),
            "room_name_dict" => Ok(Field::room_name_dict),
            "room_dict" => Ok(Field::room_dict),
            "roomid_list" => Ok(Field::roomid_list),
            "room_user" => Ok(Field::room_user),
            "user_in_room" => Ok(Field::user_in_room),
            "room_message_dict" => Ok(Field::room_message_dict),
            "room_messageid_list" => Ok(Field::room_messageid_list),

            "query_create_room" => Ok(Field::query_create_room),
            "query_enter_room" => Ok(Field::query_enter_room),
            "query_leave_room" => Ok(Field::query_leave_room),
            "query_room_users" => Ok(Field::query_room_users),
            "query_room_info" => Ok(Field::query_room_info),
            "query_room_lists" => Ok(Field::query_room_lists),
            "query_create_user" => Ok(Field::query_create_user),
            "query_user_login" => Ok(Field::query_user_login),
            "query_user_info" => Ok(Field::query_user_info),
            "query_create_message" => Ok(Field::query_create_message),
            "query_message_lists" => Ok(Field::query_message_lists),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    VEC,
    DASHMAP,
    HASHSET,
    STRING,
    NULL,
}

impl FromStr for Value {
    type Err = ();

    fn from_str(input: &str) -> Result<Value, Self::Err> {
        match input {
            "VEC" => Ok(Value::VEC),
            "DASHMAP" => Ok(Value::DASHMAP),
            "HASHSET" => Ok(Value::HASHSET),
            "STRING" => Ok(Value::STRING),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Command {
    dt: Value,
    action: Action,      //数据操作动作
    pub name: Option<Field>, //数据操作字段，对应于db.cache字段
    pub key: Option<String>, //数据操作key，如果是VEC,HASHSET,则没有
    pub val: Option<(Value, Option<Action>, Option<String>, String)>, //数据操作val, 依次为，subtype,subkey,subval
}

impl Command {
    pub const fn as_ref(&self) -> &Self {
        &self
    }

    pub fn action(&self) -> &Action {
        &self.action
    }

    pub fn is_quit(&self) -> bool {
        matches!(self.action, Action::QUIT)
    }

    pub fn is_ping(&self) -> bool {
        matches!(self.action, Action::PING)
    }

    pub fn is_pong(&self) -> bool {
        matches!(self.action, Action::PONG)
    }

    pub fn is_nop(&self) -> bool {
        matches!(self.action, Action::NULL)
    }

    pub fn is_check(&self) -> bool {
        matches!(self.action, Action::CHECK)
    }

    pub fn is_pass(&self) -> bool {
        matches!(self.action, Action::PASS)
    }

    pub fn is_offline(&self) -> bool {
        matches!(self.action, Action::OFFLINE)
    }

    pub fn is_vote(&self) -> bool {
        matches!(self.action, Action::VOTE)
    }

    pub fn is_meta(&self) -> bool {
        matches!(self.action, Action::META)
    }

    pub fn is_master(&self) -> bool {
        matches!(self.action, Action::MASTER)
    }

    pub fn is_append(&self) -> bool {
        matches!(self.action, Action::APPEND)
    }

    pub fn is_sync1(&self) -> bool {
        matches!(self.action, Action::SYNC1)
    }

    pub fn is_query(&self) -> bool {
        matches!(self.action, Action::QUERY)
    }

    pub fn is_response(&self) -> bool {
        matches!(self.action, Action::RESPONSE)
    }

    pub fn is_psync(&self) -> bool {
        matches!(self.action, Action::PSYNC)
    }

    // pub fn is_goon(&self) -> bool {
    //     matches!(self.action, Action::GOON)
    // }

    pub fn is_psfin(&self) -> bool {
        matches!(self.action, Action::PSFIN)
    }

    pub fn is_info(&self) -> bool {
        matches!(self.action, Action::INFO)
    }

    pub fn is_loip(&self) -> bool {
        matches!(self.action, Action::LOIP)
    }

    pub fn is_weak(&self) -> bool {
        matches!(self.action, Action::WEAK)
    }

    //user_dict: DashMap<String, String, FxBuildHasher>, //username => user_info
    pub fn user_dict(key: String, val: String) -> Self {
        Self {
            dt: Value::DASHMAP,
            action: Action::HSET,
            name: Some(Field::user_dict),
            key: Some(key),
            val: Some((Value::STRING, None, None, val)),
        }
    }

    //user_secret: DashMap<String, String, FxBuildHasher>, //username => password
    pub fn user_secret(key: String, val: String) -> Self {
        Self {
            dt: Value::DASHMAP,
            action: Action::HSET,
            name: Some(Field::user_secret),
            key: Some(key),
            val: Some((Value::STRING, None, None, val)),
        }
    }

    //user_token: DashMap<String, String, FxBuildHasher>, //token => username
    pub fn user_token(key: String, val: String) -> Self {
        Self {
            dt: Value::DASHMAP,
            action: Action::HSET,
            name: Some(Field::user_token),
            key: Some(key),
            val: Some((Value::STRING, None, None, val)),
        }
    }

    //room_name_dict: DashMap<String, String, FxBuildHasher>, //roomid => room_name
    pub fn room_name_dict(name: String, roomid: String) -> Self {
        Self {
            dt: Value::DASHMAP,
            action: Action::HSET,
            name: Some(Field::room_name_dict),
            key: Some(name),
            val: Some((Value::STRING, None, None, roomid)),
        }
    }

    //room_dict: DashMap<String, String, FxBuildHasher>, //roomid => room_name
    pub fn room_dict(key: String, val: String) -> Self {
        Self {
            dt: Value::DASHMAP,
            action: Action::HSET,
            name: Some(Field::room_dict),
            key: Some(key),
            val: Some((Value::STRING, None, None, val)),
        }
    }

    //roomid_list: RwLock<Vec<String>>,
    pub fn roomid_list(val: String) -> Self {
        Self {
            dt: Value::VEC,
            action: Action::PUSH,
            name: Some(Field::roomid_list),
            key: None,
            val: Some((Value::STRING, None, None, val)),
        }
    }

    pub fn init_room_user(key: String) -> Self {
        Self {
            dt: Value::DASHMAP,
            action: Action::INIT,
            name: Some(Field::room_user),
            key: Some(key),
            val: Some((Value::HASHSET, None, None, "".to_string())),
        }
    }

    //room_user: DashMap<String, HashSet<String, FxBuildHasher>, FxBuildHasher>, //roomid => user_list
    pub fn sadd_room_user(key: String, val: String) -> Self {
        Self {
            dt: Value::DASHMAP,
            action: Action::HSET,
            name: Some(Field::room_user),
            key: Some(key),
            val: Some((Value::HASHSET, Some(Action::ADD), None, val)),
        }
    }
    pub fn srem_room_user(key: String, val: String) -> Self {
        Self {
            dt: Value::DASHMAP,
            action: Action::HSET,
            name: Some(Field::room_user),
            key: Some(key),
            val: Some((Value::HASHSET, Some(Action::REM), None, val)),
        }
    }

    //user_in_room: DashMap<String, String, FxBuildHasher>,                      //token => roomid
    pub fn add_user_in_room(key: String, val: String) -> Self {
        Self {
            dt: Value::DASHMAP,
            action: Action::HSET,
            name: Some(Field::user_in_room),
            key: Some(key),
            val: Some((Value::STRING, Some(Action::ADD), None, val)),
        }
    }

    pub fn rem_user_in_room(key: String) -> Self {
        Self {
            dt: Value::DASHMAP,
            action: Action::HSET,
            name: Some(Field::user_in_room),
            key: Some(key),
            val: Some((Value::STRING, Some(Action::REM), None, "".to_string())),
        }
    }

    //room_message_dict: DashMap<String, DashMap<String, String, FxBuildHasher>, FxBuildHasher>, //roomid => message_dict
    pub fn room_message_dict(key: String, val: (String, String)) -> Self {
        Self {
            dt: Value::DASHMAP,
            action: Action::HSET,
            name: Some(Field::room_message_dict),
            key: Some(key),
            val: Some((Value::DASHMAP, Some(Action::HSET), Some(val.0), val.1)),
        }
    }

    //room_messageid_list: DashMap<String, Vec<String>, FxBuildHasher>, //roomid => messageid_list
    pub fn room_messageid_list(key: String, val: String) -> Self {
        Self {
            dt: Value::DASHMAP,
            action: Action::HSET,
            name: Some(Field::room_messageid_list),
            key: Some(key),
            val: Some((Value::VEC, Some(Action::PUSH), None, val)),
        }
    }

    pub fn quit() -> Self {
        Self {
            dt: Value::NULL,
            action: Action::QUIT,
            name: None,
            key: None,
            val: None,
        }
    }

    pub fn ping() -> Self {
        Self {
            dt: Value::NULL,
            action: Action::PING,
            name: None,
            key: None,
            val: None,
        }
    }

    pub fn pong() -> Self {
        Self {
            dt: Value::NULL,
            action: Action::PONG,
            name: None,
            key: None,
            val: None,
        }
    }

    pub fn nop() -> Self {
        Self {
            dt: Value::NULL,
            action: Action::NULL,
            name: None,
            key: None,
            val: None,
        }
    }

    pub fn check(ip: String, kind: String) -> Self {
        Self {
            dt: Value::NULL,
            action: Action::CHECK,
            name: None,
            key: Some(ip),
            val: Some((Value::NULL, None, None, kind)),
        }
    }

    pub fn pass() -> Self {
        Self {
            dt: Value::NULL,
            action: Action::PASS,
            name: None,
            key: None,
            val: None,
        }
    }

    #[allow(dead_code)]
    pub fn offline() -> Self {
        Self {
            dt: Value::NULL,
            action: Action::OFFLINE,
            name: None,
            key: None,
            val: None,
        }
    }

    pub fn vote(ip: String) -> Self {
        Self {
            dt: Value::NULL,
            action: Action::VOTE,
            name: None,
            key: Some(ip),
            val: None,
        }
    }

    /*
    pub offset: usize,       //数据同步偏移量
    pub last_update: i64,    //最后一次更新时间戳
    pub last_ping: i64,      //最后一次接受ping的时间
    pub birthtime: i64,      //记录产生时间
    */
    pub fn meta(offset: usize, last_update: i64, last_ping: i64, birthtime: i64) -> Self {
        Self {
            dt: Value::NULL,
            action: Action::META,
            name: None,
            key: Some(format!(
                "{}-{}-{}-{}",
                offset, last_update, last_ping, birthtime
            )),
            val: None,
        }
    }

    pub fn master(ip: String) -> Self {
        Self {
            dt: Value::NULL,
            action: Action::MASTER,
            name: None,
            key: Some(ip),
            val: None,
        }
    }

    pub fn append(cmd: Command) -> Self {
        Self {
            dt: Value::NULL,
            action: Action::APPEND,
            name: None,
            key: None,
            val: Some((Value::NULL, None, None, cmd.encode().trim_end_matches('\n').to_string())),
        }
    }

    //用于向slave持久化指令
    pub fn sync1(cmd: Command) -> Self {
        Self {
            dt: Value::NULL,
            action: Action::SYNC1,
            name: None,
            key: None,
            val: Some((Value::NULL, None, None, cmd.encode().trim_end_matches('\n').to_string())),
        }
    }

    //查询指令
    pub fn query(name: Field, key: String) -> Self {
        Self {
            dt: Value::NULL,
            action: Action::QUERY,
            name: Some(name),
            key: Some(key),
            val: None,
        }
    }

    pub fn response(res: String, errinfo: String) -> Self {
        Self {
            dt: Value::NULL,
            action: Action::RESPONSE,
            name: None,
            key: Some(res),
            val: Some((Value::NULL, None, None, errinfo)),
        }
    }

    //用于主从服务器断线重连的过程中，重新同步的指令
    pub fn psync(cmd: String) -> Self {
        Self {
            dt: Value::NULL,
            action: Action::PSYNC,
            name: None,
            key: None,
            val: Some((Value::NULL, None, None, cmd)),
        }
    }

    // pub fn goon() -> Self {
    //     Self {
    //         dt: Value::NULL,
    //         action: Action::GOON,
    //         name: None,
    //         key: None,
    //         val: None,
    //     }
    // }

    pub fn psfin() -> Self {
        Self {
            dt: Value::NULL,
            action: Action::PSFIN,
            name: None,
            key: None,
            val: None,
        }
    }

    //使用","连接ip，然后发送
    pub fn info(ips: String) -> Self {
        Self {
            dt: Value::NULL,
            action: Action::INFO,
            name: None,
            key: Some(ips),
            val: None,
        }
    }

    pub fn loip(ip: String) -> Self {
        Self {
            dt: Value::NULL,
            action: Action::LOIP,
            name: None,
            key: Some(ip),
            val: None,
        }
    }

    pub fn weak() -> Self {
        Self {
            dt: Value::NULL,
            action: Action::WEAK,
            name: None,
            key: None,
            val: None,
        }
    }

    pub fn encode(self) -> String {
        let val = self
            .val
            .unwrap_or((Value::NULL, None, None, "".to_string()));
        format!(
            "{:?}\r{:?}\r{:?}\r{}\r{:?}\r{:?}\r{}\r{}\n",
            self.dt,
            self.action,
            if let Some(name) = self.name {
                name
            } else {
                Field::null
            },
            if let Some(key) = self.key {
                key
            } else {
                NONE.to_owned()
            },
            val.0,
            if let Some(va) = val.1 {
                va
            } else {
                Action::NULL
            },
            if let Some(vk) = val.2 {
                vk
            } else {
                NONE.to_owned()
            },
            val.3
        )
    }

    pub fn decode(line: &str) -> crate::Result<Self> {
        let mut buf = Cursor::new(&line.as_bytes()[..]);
        buf.set_position(0);

        let mut s = vec![];
        let end = buf.get_ref().len();
        let mut pos = 0;
        let mut count = 0;

        for i in 0..end {
            if buf.get_ref()[i] == b'\r' {
                buf.set_position(i as u64);
                s.push(str::from_utf8(&buf.get_ref()[pos..i])?);
                pos = i + 1;
                count += 1;
            }
            if count + 1 == 8 {
                s.push(str::from_utf8(&buf.get_ref()[pos..])?.trim_end_matches('\n'));//
                break;
            }
        }

        debug!("{:?}", s);
        // let s1 = line.split('\r').collect::<Vec<&str>>();
        // debug!("{:?}", s1);
        Ok(Self {
            dt: if let Ok(dt) = Value::from_str(s[0]) {
                dt
            } else {
                Value::NULL
            },
            action: if let Ok(action) = Action::from_str(s[1]) {
                action
            } else {
                Action::NULL
            },
            name: if let Ok(name) = Field::from_str(s[2]) {
                Some(name)
            } else {
                None
            },
            key: if s[3] == NONE {
                None
            } else {
                Some(s[3].to_owned())
            },
            val: Some((
                if let Ok(vt) = Value::from_str(s[4]) {
                    vt
                } else {
                    Value::NULL
                },
                if let Ok(va) = Action::from_str(s[5]) {
                    Some(va)
                } else {
                    None
                },
                if s[6] == NONE {
                    None
                } else {
                    Some(s[6].to_owned())
                },
                s[7].to_owned(),
            )),
        })
    }

    pub fn restore(self, db: &Db) {
        match self.name {
            Some(Field::user_dict) => {
                if let Action::HSET = self.action {
                    db.cache
                        .user_dict
                        .insert(self.key.unwrap(), self.val.unwrap().3);
                }
            }
            Some(Field::user_secret) => {
                if let Action::HSET = self.action {
                    db.cache
                        .user_secret
                        .insert(self.key.unwrap(), self.val.unwrap().3);
                }
            }
            Some(Field::user_token) => {
                if let Action::HSET = self.action {
                    db.cache
                        .user_token
                        .insert(self.key.unwrap(), self.val.unwrap().3);
                }
            }
            Some(Field::room_name_dict) => {
                if let Action::HSET = self.action {
                    db.cache
                        .room_name_dict
                        .insert(self.key.unwrap(), self.val.unwrap().3);
                }
            }
            Some(Field::room_dict) => {
                if let Action::HSET = self.action {
                    db.cache
                        .room_dict
                        .insert(self.key.unwrap(), self.val.unwrap().3);
                }
            }
            Some(Field::roomid_list) => {
                if let Action::PUSH = self.action {
                    db.cache.roomid_list.write().push(self.val.unwrap().3);
                }
            }
            Some(Field::room_user) => match self.action {
                Action::HSET => {
                    let key = self.key.unwrap();
                    let val = self.val.unwrap();
                    match val.1 {
                        Some(Action::ADD) => {
                            let userset = db.cache.room_user.get_mut(&key);
                            if let Some(mut userset) = userset {
                                userset.insert(val.3);
                            }
                        }
                        Some(Action::REM) => {
                            let userset = db.cache.room_user.get_mut(&key);
                            if let Some(mut userset) = userset {
                                userset.remove(&val.3);
                            }
                        }
                        _ => {}
                    }
                }
                Action::INIT => {
                    let key = self.key.unwrap();
                    db.cache
                        .room_user
                        .insert(key, HashSet::with_hasher(FxBuildHasher::default()));
                }
                _ => {}
            },
            Some(Field::user_in_room) => {
                if let Action::HSET = self.action {
                    db.cache
                        .user_in_room
                        .insert(self.key.unwrap(), self.val.unwrap().3);
                }
            }
            Some(Field::room_message_dict) => {
                if let Action::HSET = self.action {
                    let key = self.key.unwrap();
                    let val = self.val.unwrap();
                    if let Some(Action::HSET) = val.1 {
                        let message_dict = db.cache.room_message_dict.get_mut(&key);
                        if let Some(message_dict) = message_dict {
                            message_dict.insert(val.2.unwrap(), val.3);
                        } else {
                            let message_dict = DashMap::with_hasher(FxBuildHasher::default());
                            message_dict.insert(val.2.unwrap(), val.3);
                            db.cache.room_message_dict.insert(key, message_dict);
                        }
                    }
                }
            }
            Some(Field::room_messageid_list) => {
                if let Action::HSET = self.action {
                    let key = self.key.unwrap();
                    let val = self.val.unwrap();
                    if let Some(Action::PUSH) = val.1 {
                        let messageid_list = db.cache.room_messageid_list.get_mut(&key);
                        if let Some(mut messageid_list) = messageid_list {
                            messageid_list.push(val.3);
                        } else {
                            db.cache.room_messageid_list.insert(key, vec![val.3]);
                        }
                    }
                }
            }
            _ => {}
        }
    }
}
