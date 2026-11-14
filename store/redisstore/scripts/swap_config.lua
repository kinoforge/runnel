local key = k("cfg:" .. ARGV[1])
local cur = redis.call("HGET", key, ARGV[2])
if ARGV[3] == "1" then
  if cur ~= ARGV[4] then return 0 end
elseif cur then
  return 0
end
redis.call("HSET", key, ARGV[2], ARGV[5])
return 1
