#!/bin/sh

redis_cli=../build/redis-cli

# enter_room_hash=$(${redis_cli} -x script load < ./enter_room.lua)

# echo $enter_room_hash

# ../build/redis-cli -x script load < ./leave_room.lua
../build/redis-cli -x script load < ./query_room.lua
# ../build/redis-cli -x script load < ./user_create.lua
# ../build/redis-cli -x script load < ./user_login.lua
# ../build/redis-cli -x script load < ./message_create.lua
# ../build/redis-cli -x script load < ./message_query.lua
