local now = tonumber(ARGV[1])
local lease = tonumber(ARGV[2])
local worker = ARGV[3]
local limit = tonumber(ARGV[4])
local scan = tonumber(ARGV[5])
local queues = cjson.decode(ARGV[6])
local only = cjson.decode(ARGV[7])
local out = {}
for _, q in ipairs(queues) do
  if #out >= limit or #only > 0 then break end
  local due = redis.call("ZRANGEBYSCORE", k("poll:" .. q), "-inf", s(now), "LIMIT", 0, s(limit - #out))
  for _, id in ipairs(due) do
    local j = load(id)
    if j then
      unindex(j)
      j.state = "running"
      j.worker = worker
      j.lease_until = s(lease)
      j.version = s(num(j, "version") + 1)
      j.updated_at = s(now)
      save(j)
      index(j, now)
      out[#out + 1] = vals(j)
    end
  end
end
if #out >= limit then return out end
local function cfg(kind, name)
  local raw = redis.call("HGET", k("cfg:" .. kind), name)
  if raw then return cjson.decode(raw) end
  return {}
end
local function n(v) return tonumber(v) or 0 end
local qcfg, pcfg = {}, {}
local open = {}
for _, q in ipairs(queues) do
  qcfg[q] = cfg("queue", q)
  if not qcfg[q].paused and #only == 0 then
    local due = redis.call("ZRANGEBYSCORE", k("sched:" .. q), "-inf", s(now), "LIMIT", 0, 1000)
    for _, id in ipairs(due) do
      local v = redis.call("HMGET", k("j:" .. id), "priority", "run_at")
      redis.call("ZREM", k("sched:" .. q), id)
      if v[1] then
        redis.call("ZADD", k("ready:" .. q), s(ready_score({priority = v[1], run_at = v[2]})), id)
      end
    end
  end
  if not qcfg[q].paused then open[#open + 1] = q end
end
local function act(f) return tonumber(redis.call("HGET", k("act"), f) or "0") end
local buckets = {}
local function bucket(key, rate, burst)
  local b = buckets[key]
  if not b then
    local capacity = burst
    if capacity <= 0 then capacity = math.max(1, math.ceil(rate)) end
    local tokens = capacity
    local raw = redis.call("HGET", k("bk"), key)
    if raw then
      local t, at = string.match(raw, "^([^|]+)|(.+)$")
      tokens = math.min(capacity, tonumber(t) + math.max(now - tonumber(at), 0) * rate / 1000)
    end
    b = {t = tokens, dirty = false}
    buckets[key] = b
  end
  return b
end
local fullq, fullqt, fullp, fullpt = {}, {}, {}, {}
local offset = {}
local function verdict(j)
  local q = qcfg[j.queue]
  if fullq[j.queue] or fullqt[j.queue .. "|" .. j.tenant] or fullp[j.pool] or fullpt[j.pool .. "|" .. j.tenant] then return "skip" end
  if n(q.concurrency) > 0 and act("q|" .. j.queue) >= n(q.concurrency) then fullq[j.queue] = true; return "skip" end
  if j.tenant ~= "" and n(q.tenant_slots) > 0 and act("qt|" .. j.queue .. "|" .. j.tenant) >= n(q.tenant_slots) then
    fullqt[j.queue .. "|" .. j.tenant] = true; return "skip"
  end
  if j.pool ~= "" then
    if pcfg[j.pool] == nil then pcfg[j.pool] = cfg("pool", j.pool) end
    local p = pcfg[j.pool]
    if n(p.slots) > 0 and act("p|" .. j.pool) >= n(p.slots) then fullp[j.pool] = true; return "skip" end
    if j.tenant ~= "" and n(p.tenant_slots) > 0 and act("pt|" .. j.pool .. "|" .. j.tenant) >= n(p.tenant_slots) then
      fullpt[j.pool .. "|" .. j.tenant] = true; return "skip"
    end
  end
  local take = {}
  if n(q.rate) > 0 then
    local b = bucket("q|" .. j.queue, n(q.rate), n(q.burst))
    if b.t < 1 then fullq[j.queue] = true; return "skip" end
    take[#take + 1] = b
  end
  if j.tenant ~= "" and n(q.tenant_rate) > 0 then
    local b = bucket("qt|" .. j.queue .. "|" .. j.tenant, n(q.tenant_rate), n(q.tenant_burst))
    if b.t < 1 then fullqt[j.queue .. "|" .. j.tenant] = true; return "skip" end
    take[#take + 1] = b
  end
  for _, b in ipairs(take) do b.t = b.t - 1; b.dirty = true end
  return "ok"
end
local examined = 0
local page = scan
for round = 1, 64 do
  if #out >= limit or examined >= 2000 then break end
  local cands = {}
  for _, q in ipairs(open) do
    if not fullq[q] then
      if #only > 0 then
        if round == 1 then
          for _, id in ipairs(only) do
            local sc = redis.call("ZSCORE", k("ready:" .. q), id)
            if sc then cands[#cands + 1] = {id = id, score = tonumber(sc), q = q} end
          end
        end
      else
        local from = offset[q] or 0
        local r = redis.call("ZRANGE", k("ready:" .. q), s(from), s(from + page - 1), "WITHSCORES")
        for i = 1, #r, 2 do cands[#cands + 1] = {id = r[i], score = tonumber(r[i + 1]), q = q} end
      end
    end
  end
  if #cands == 0 then break end
  table.sort(cands, function(a, b)
    if a.score ~= b.score then return a.score < b.score end
    return a.id < b.id
  end)
  local skipped = false
  local taken = {}
  for _, c in ipairs(cands) do
    examined = examined + 1
    if #out >= limit then break end
    local v = redis.call("HMGET", k("j:" .. c.id), "state", "queue", "tenant", "pool")
    if v[1] == "waiting" then
      local j = {queue = v[2], tenant = v[3] or "", pool = v[4] or ""}
      if verdict(j) == "ok" then
        local full = load(c.id)
        unindex(full)
        full.state = "running"
        full.attempt = s(num(full, "attempt") + 1)
        full.worker = worker
        full.lease_until = s(lease)
        full.version = s(num(full, "version") + 1)
        full.updated_at = s(now)
        if num(full, "started_at") == 0 then full.started_at = s(now) end
        save(full)
        index(full, now)
        out[#out + 1] = vals(full)
        taken[c.q] = (taken[c.q] or 0) + 1
      else
        skipped = true
      end
    end
  end
  if not skipped or #only > 0 then break end
  for _, q in ipairs(open) do
    local seen = 0
    for _, c in ipairs(cands) do if c.q == q then seen = seen + 1 end end
    offset[q] = (offset[q] or 0) + seen - (taken[q] or 0)
  end
  page = math.min(page * 2, 500)
end
for key, b in pairs(buckets) do
  if b.dirty then redis.call("HSET", k("bk"), key, tostring(b.t) .. "|" .. s(now)) end
end
return out
