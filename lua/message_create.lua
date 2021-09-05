-- let now = Local::now().timestamp().to_string();
-- self.timestamp = Some(now);
-- let val = serde_json::to_string(self)?;

-- let user_in_room_key = format!("{}{}", token, USER_IN_ROOM);
-- let roomid: String = conn.get(user_in_room_key).await?;

-- let room_messageid_list = format!("{}:{}", roomid.as_str(), MESSAGEID_LIST);
-- conn.rpush(room_messageid_list, self.id.as_str()).await?;
-- let room_message_dict = format!("{}:{}", roomid.as_str(), MESSAGE_DICT);
-- conn.hset(room_message_dict, self.id.as_str(), val.as_str()).await?;

-- Ok(())

local token = KEYS[1]
local val = KEYS[2]
local id = KEYS[3]

local user_in_room_key = token .. ":user_in_room"
local roomid = redis.call("GET", user_in_room_key)
local room_messageid_list = roomid .. ":messageid_list"
redis.call("RPUSH", room_messageid_list, id)
local room_message_dict = roomid .. ":message_dict"
redis.call("HSET", room_message_dict, id, val)
