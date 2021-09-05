-- let password = self.password.as_ref().map(|password| {
--   let mut hasher = Sha1::new();
--   hasher.update(password.as_bytes());
--   hasher.hexdigest()
-- }).unwrap();
-- let val = serde_json::to_string(self)?;
-- conn.hset(USER_DICT, self.username.as_str(), val.as_str()).await?;
-- conn.hset(USER_SECRET, self.username.as_str(), password.as_str()).await?;

-- Ok(())

local username = KEYS[1]
local password = KEYS[2]
local val = KEYS[3]

redis.call("HSET", "user_dict", username, val)
redis.call("HSET", "user_secret", username, password)
