local until_at = ARGV[1]
local worker = ARGV[2]
local ids = cjson.decode(ARGV[3])
local lost = {}
for _, id in ipairs(ids) do
  local v = redis.call("HMGET", k("j:" .. id), "state", "worker")
  if v[1] == "running" and v[2] == worker then
    redis.call("HSET", k("j:" .. id), "lease_until", until_at)
    redis.call("ZADD", k("leases"), until_at, id)
  else
    lost[#lost + 1] = id
  end
end
return lost
