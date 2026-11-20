local before = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local states = cjson.decode(ARGV[3])
local queue = ARGV[4]
local archive = ARGV[5] == "1"
local sweep = tonumber(ARGV[6])
local n = 0
local old = redis.call("ZRANGE", k("fin"), 0, sweep - 1, "WITHSCORES")
for i = 1, #old, 2 do
  local id = old[i]
  local st = redis.call("HGET", k("j:" .. id), "state")
  if st and final(st) then redis.call("ZADD", k("fin:" .. st), old[i + 1], id) end
  redis.call("ZREM", k("fin"), id)
end
local function prune_key(zkey)
  local offset = 0
  local page = math.min(math.max(limit * 2, 100), 1000)
  for _ = 1, 20 do
    if n >= limit then break end
    local ids = redis.call("ZRANGEBYSCORE", zkey, "-inf", "(" .. s(before), "LIMIT", s(offset), s(page))
    if #ids == 0 then break end
    local removed = 0
    for _, id in ipairs(ids) do
      if n >= limit then break end
      local j = load(id)
      if not j then
        redis.call("ZREM", zkey, id)
        removed = removed + 1
      elseif queue == "" or j.queue == queue then
        if archive then
          redis.call("LPUSH", k("archive"), cjson.encode(j))
          redis.call("LTRIM", k("archive"), 0, 99999)
        end
        unindex(j)
        redis.call("ZREM", k("all"), id)
        redis.call("DEL", k("j:" .. id), k("kids:" .. id))
        n = n + 1
        removed = removed + 1
      end
    end
    if #ids < page then break end
    offset = offset + #ids - removed
  end
end
for _, st in ipairs(states) do
  if n >= limit then break end
  prune_key(k("fin:" .. st))
end
local function expire_unique(ukey, cur)
  local _, until_at = string.match(cur, "^([^|]+)|(.+)$")
  until_at = tonumber(until_at) or 0
  if until_at > 0 and until_at < before then
    redis.call("HDEL", k("uniq"), ukey)
    redis.call("ZREM", k("uniq_exp"), ukey)
  elseif until_at > 0 then
    redis.call("ZADD", k("uniq_exp"), s(until_at), ukey)
  else
    redis.call("ZREM", k("uniq_exp"), ukey)
  end
end
local scan = redis.call("HSCAN", k("uniq"), redis.call("GET", k("uniq_scan")) or "0", "COUNT", sweep)
redis.call("SET", k("uniq_scan"), scan[1])
for i = 1, #scan[2], 2 do expire_unique(scan[2][i], scan[2][i + 1]) end
local expired = redis.call("ZRANGEBYSCORE", k("uniq_exp"), "-inf", "(" .. s(before), "LIMIT", 0, sweep)
for _, ukey in ipairs(expired) do
  local cur = redis.call("HGET", k("uniq"), ukey)
  if cur then expire_unique(ukey, cur) else redis.call("ZREM", k("uniq_exp"), ukey) end
end
return n
