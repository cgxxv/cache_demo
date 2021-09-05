-- let hash_value: String = conn.hget(USER_SECRET, username).await?;

-- let mut hasher = Sha1::new();
-- hasher.update(password.as_bytes());
-- let repassword = hasher.hexdigest();
-- if hash_value == repassword {
--     let token = Uuid::new_v4().to_string();
--     conn.hset(USER_TOKEN, token.as_bytes(), username).await?;
--     return Ok(token)
-- }

-- Ok("".to_string())

local username = KEYS[1]
local password = KEYS[2]
local token = KEYS[3]

local repassword = redis.call("HGET", "user_secret", username)
if password == repassword then
  redis.call("HSET", "user_token", token, username)
  return token
end

return ""
