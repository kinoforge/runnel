local before = tonumber(ARGV[1])
local upto = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local max = "+inf"
if upto > 0 then max = s(upto) end
local rows = redis.call("ZRANGEBYSCORE", k("events"), "-inf", max, "LIMIT", 0, s(limit))
local n = 0
for _, raw in ipairs(rows) do
  local e = cjson.decode(raw)
  if tonumber(e.at) >= before then break end
  redis.call("ZREM", k("events"), raw)
  n = n + 1
end
return n
