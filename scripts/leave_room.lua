-- let username: String = conn.hget(USER_TOKEN, token).await?;
-- let user_in_room_key = format!("{}{}", token, USER_IN_ROOM);
-- let roomid: String = conn.get(user_in_room_key.as_str()).await?;
-- let room_user_set = format!("{}:{}", roomid.as_str(), ROOM_USER);
-- conn.srem(room_user_set, username).await?;
-- conn.del(user_in_room_key).await?;

local token = KEYS[1]

local username = redis.call("HGET", "user_token", token)
local user_in_room_key = token .. ":user_in_room"
local roomid = redis.call("GET", user_in_room_key)
local room_user_set = roomid .. ":roomid_user"
redis.call("SREM", room_user_set, username)
redis.call("DEL", user_in_room_key)
