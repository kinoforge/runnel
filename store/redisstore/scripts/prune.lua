local before = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local states = cjson.decode(ARGV[3])
local queue = ARGV[4]
local archive = ARGV[5] == "1"
local want = {}
for _, st in ipairs(states) do want[st] = true end
local ids = redis.call("ZRANGEBYSCORE", k("fin"), "-inf", "(" .. s(before), "LIMIT", 0, s(limit * 5))
local n = 0
for _, id in ipairs(ids) do
  if n >= limit then break end
  local j = load(id)
  if not j then
    redis.call("ZREM", k("fin"), id)
  elseif want[j.state] and (queue == "" or j.queue == queue) then
    if archive then
      redis.call("LPUSH", k("archive"), cjson.encode(j))
      redis.call("LTRIM", k("archive"), 0, 99999)
    end
    unindex(j)
    redis.call("ZREM", k("all"), id)
    redis.call("DEL", k("j:" .. id), k("kids:" .. id))
    n = n + 1
  end
end
local uniq = redis.call("HGETALL", k("uniq"))
for i = 1, #uniq, 2 do
  local _, until_at = string.match(uniq[i + 1], "^([^|]+)|(.+)$")
  until_at = tonumber(until_at)
  if until_at > 0 and until_at < before then redis.call("HDEL", k("uniq"), uniq[i]) end
end
return n
