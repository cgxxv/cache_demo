-- let id = Uuid::new_v4().to_string();
-- conn.rpush(ROOMID_LIST, id.as_str()).await?;
-- conn.hset(ROOM_DICT, id.as_str(), self.name.as_str()).await?;

local roomid = KEYS[1]
local name = KEYS[2]

redis.call("RPUSH", "roomid_list", roomid)
redis.call("HSET", "room_dict", roomid, name)
