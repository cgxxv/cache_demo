-- let roomids: Vec<Vec<u8>> = conn.lrange(ROOMID_LIST, index, size).await?;

-- let mut data = vec![];
-- if roomids.len() == 1 {
--     let v: String = conn.hget(ROOM_DICT, &roomids[..]).await?;
--     data.push(Room{
--         name: v,
--         id: Some(String::from_utf8(roomids[0].to_vec()).unwrap()),
--     })
-- } else if roomids.len() > 1 {
--     let vs: Vec<String> = conn.hget(ROOM_DICT, &roomids[..]).await?;
--     for (i,roomid) in roomids.iter().enumerate() {
--         data.push(Room{
--             name: vs[i].clone(),
--             id: Some(String::from_utf8(roomid.to_vec()).unwrap()),
--         });
--     }
-- }

local index = 0
local size = -1

local roomids = redis.call("LRANGE", "roomid_list", index, size)
if roomids == nil then
  roomids = {}
end

local res = {}
if table.getn(roomids) > 0 then
  res = redis.call("HMGET", "room_dict", unpack(roomids))
end

return {roomids, res}
