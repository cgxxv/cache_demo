-- let room_user_set = format!("{}:{}", roomid, ROOM_USER);
-- let username: String = conn.hget(USER_TOKEN, token).await?;
-- conn.sadd(room_user_set, username).await?;
-- let user_in_room_key = format!("{}{}", token, USER_IN_ROOM);
-- conn.set(user_in_room_key, roomid).await?;

local roomid = KEYS[1]
local token = KEYS[2]

local room_user_set = roomid .. ":roomid_user"
local username = redis.call("HGET", "user_token", token)
redis.call("SADD", room_user_set, username)
local user_in_room_key = token .. ":user_in_room"
redis.call("SET", user_in_room_key, roomid)
