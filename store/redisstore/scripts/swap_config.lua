local key = k("cfg:" .. ARGV[1])
local cur = redis.call("HGET", key, ARGV[2])
if ARGV[3] == "1" then
  if cur ~= ARGV[4] then return 0 end
elseif cur then
  return 0
end
redis.call("HSET", key, ARGV[2], ARGV[5])
if ARGV[1] == "pool" then
  if ARGV[6] and ARGV[6] ~= "" then
    redis.call("HSET", k("cfg:pool_pause"), ARGV[2], ARGV[6])
  else
    redis.call("HDEL", k("cfg:pool_pause"), ARGV[2])
  end
end
return 1
