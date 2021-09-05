-- let user_in_room_key = format!("{}{}", token, USER_IN_ROOM);
-- let roomid: String = conn.get(user_in_room_key).await?;

-- let room_messageid_list = format!("{}:{}", roomid.as_str(), MESSAGEID_LIST);
-- let length: isize = conn.llen(room_messageid_list.as_str()).await?;
-- let stop = length + index;
-- let start = stop - size + 1;
-- let message_ids: Vec<Vec<u8>> = conn.lrange(room_messageid_list, start, stop).await?;

-- let mut data = vec![];
-- let room_message_dict = format!("{}:{}", roomid.as_str(), MESSAGE_DICT);
-- if length == 1{
--     let v: Vec<u8> = conn.hget(room_message_dict, &message_ids[..]).await?;
--     let message: Message = serde_json::from_slice(&v)?;
--     data.push(message);
-- } else if length > 1 {
--     let vs: Vec<Vec<u8>> = conn.hget(room_message_dict, &message_ids[..]).await?;
--     for v in vs {
--         let message: Message = serde_json::from_slice(&v)?;
--         data.push(message);
--     }
-- }

local index = KEYS[1]
local size = KEYS[2]
local token = KEYS[3]

local user_in_room_key = token .. ":user_in_room"
local roomid = redis.call("GET", user_in_room_key)
local room_messageid_list = roomid .. ":messageid_list"
local length = redis.call("LLEN", room_messageid_list)
local stop = length + index
local start = stop - size + 1
local message_ids = redis.call("LRANGE", room_messageid_list, start, stop)
local room_message_dict = roomid .. ":message_dict"

local res = {}
if table.getn(message_ids) > 0 then
  res = redis.call("HMGET", room_message_dict, unpack(message_ids))
end

return res
