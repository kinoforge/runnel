--!df flags=allow-undeclared-keys
local unpack = unpack or table.unpack
local P = KEYS[1]
local function k(s) return P .. s end
local F = {"id","queue","kind","state","priority","group","pool","attempt","max_attempts","run_at","waiting_since","start_by","start_timeout","deadline","worker","lease_until","version","pending","unique_key","created_at","updated_at","started_at","finished_at","data","deps"}
local function s(n) return string.format("%.0f", n) end
local function num(j, f) return tonumber(j[f]) or 0 end
local function load(id)
  local v = redis.call("HMGET", k("j:" .. id), unpack(F))
  if not v[1] then return nil end
  local j = {}
  for i, f in ipairs(F) do j[f] = v[i] or "" end
  return j
end
local function vals(j)
  local out = {}
  for i, f in ipairs(F) do out[i] = j[f] end
  return out
end
local function save(j)
  local a = {}
  for _, f in ipairs(F) do a[#a + 1] = f; a[#a + 1] = j[f] end
  redis.call("HSET", k("j:" .. j.id), unpack(a))
end
local function final(st) return st == "completed" or st == "dead" or st == "cancelled" or st == "expired" end
local function ready_score(j) return (1000 - num(j, "priority")) * 4398046511104 + num(j, "run_at") end
local function active(j, d)
  redis.call("HINCRBY", k("act"), "q|" .. j.queue, d)
  if j.group ~= "" then redis.call("HINCRBY", k("act"), "qt|" .. j.queue .. "|" .. j.group, d) end
  if j.pool ~= "" then
    redis.call("HINCRBY", k("act"), "r|" .. j.pool, d)
    if j.group ~= "" then redis.call("HINCRBY", k("act"), "rt|" .. j.pool .. "|" .. j.group, d) end
  end
end
local function waitc(j, d)
  if j.group ~= "" then redis.call("HINCRBY", k("wait"), j.queue .. "|" .. j.group, d) end
end
local function index(j, now)
  local st = j.state
  redis.call("ZADD", k("st:" .. st), 0, j.id)
  if not final(st) and num(j, "deadline") > 0 then redis.call("ZADD", k("deadlines"), s(num(j, "deadline")), j.id) end
  redis.call("HINCRBY", k("cnt"), j.queue .. "|" .. st, 1)
  if st == "waiting" then
    if num(j, "run_at") > now then
      redis.call("ZADD", k("sched:" .. j.queue), s(num(j, "run_at")), j.id)
    else
      redis.call("ZADD", k("ready:" .. j.queue), s(ready_score(j)), j.id)
    end
    if num(j, "start_by") > 0 then redis.call("ZADD", k("startby"), s(num(j, "start_by")), j.id) end
    waitc(j, 1)
  elseif st == "blocked" then
    waitc(j, 1)
  elseif st == "running" then
    redis.call("ZADD", k("leases"), s(num(j, "lease_until")), j.id)
    active(j, 1)
  elseif st == "polling" then
    redis.call("ZADD", k("poll:" .. j.queue), s(num(j, "run_at")), j.id)
    active(j, 1)
  else
    redis.call("ZADD", k("fin:" .. st), s(num(j, "finished_at")), j.id)
  end
end
local function unindex(j)
  local st = j.state
  redis.call("ZREM", k("st:" .. st), j.id)
  redis.call("ZREM", k("deadlines"), j.id)
  redis.call("HINCRBY", k("cnt"), j.queue .. "|" .. st, -1)
  if st == "waiting" then
    redis.call("ZREM", k("sched:" .. j.queue), j.id)
    redis.call("ZREM", k("ready:" .. j.queue), j.id)
    redis.call("ZREM", k("startby"), j.id)
    waitc(j, -1)
  elseif st == "blocked" then
    waitc(j, -1)
  elseif st == "running" then
    redis.call("ZREM", k("leases"), j.id)
    active(j, -1)
  elseif st == "polling" then
    redis.call("ZREM", k("poll:" .. j.queue), j.id)
    active(j, -1)
  else
    redis.call("ZREM", k("fin"), j.id)
    redis.call("ZREM", k("fin:" .. st), j.id)
  end
end
local function wake() redis.call("PUBLISH", P .. "wake", "1") end
local finalize
finalize = function(j, now)
  local seq = redis.call("INCR", k("evseq"))
  redis.call("ZADD", k("events"), s(seq), cjson.encode({seq = seq, id = j.id, queue = j.queue, kind = j.kind, state = j.state, at = now}))
  redis.call("PUBLISH", P .. "end", j.id)
  if j.unique_key ~= "" then
    local cur = redis.call("HGET", k("uniq"), j.unique_key)
    if cur and cur == j.id .. "|0" then redis.call("HDEL", k("uniq"), j.unique_key) end
  end
  local kids = redis.call("SMEMBERS", k("kids:" .. j.id))
  redis.call("DEL", k("kids:" .. j.id))
  local released = false
  for _, cid in ipairs(kids) do
    local c = load(cid)
    if c and c.state == "blocked" then
      c.version = s(num(c, "version") + 1)
      c.updated_at = s(now)
      if j.state == "completed" then
        local p = num(c, "pending") - 1
        c.pending = s(p)
        if p <= 0 then
          unindex(c)
          c.state = "waiting"
          local base = math.max(num(c, "run_at"), now)
          c.run_at = s(base)
          c.waiting_since = s(base)
          local sw = num(c, "start_timeout")
          if sw > 0 then c.start_by = s(base + sw) else c.start_by = "0" end
          save(c)
          index(c, now)
          released = true
        else
          save(c)
        end
      else
        unindex(c)
        c.state = "cancelled"
        c.pending = "0"
        c.finished_at = s(now)
        c.start_by = "0"
        save(c)
        index(c, now)
        finalize(c, now)
      end
    end
  end
  if released then wake() end
end
