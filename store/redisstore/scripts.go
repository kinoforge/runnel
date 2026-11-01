package redisstore

const common = `--!df flags=allow-undeclared-keys
local unpack = unpack or table.unpack
local P = KEYS[1]
local function k(s) return P .. s end
local F = {"id","queue","kind","state","priority","tenant","pool","attempt","max_attempts","run_at","waiting_since","start_by","start_within","worker","lease_until","version","pending","unique_key","created_at","updated_at","started_at","finished_at","data","deps"}
local function s(n) return string.format("%d", n) end
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
local function final(st) return st == "completed" or st == "dead" or st == "cancelled" end
local function ready_score(j) return (1000 - num(j, "priority")) * 1099511627776 + math.floor(num(j, "run_at") / 1000) end
local function active(j, d)
  redis.call("HINCRBY", k("act"), "q|" .. j.queue, d)
  if j.tenant ~= "" then redis.call("HINCRBY", k("act"), "qt|" .. j.queue .. "|" .. j.tenant, d) end
  if j.pool ~= "" then
    redis.call("HINCRBY", k("act"), "p|" .. j.pool, d)
    if j.tenant ~= "" then redis.call("HINCRBY", k("act"), "pt|" .. j.pool .. "|" .. j.tenant, d) end
  end
end
local function waitc(j, d)
  if j.tenant ~= "" then redis.call("HINCRBY", k("wait"), j.queue .. "|" .. j.tenant, d) end
end
local function index(j, now)
  local st = j.state
  redis.call("ZADD", k("st:" .. st), 0, j.id)
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
    redis.call("ZADD", k("fin"), s(num(j, "finished_at")), j.id)
  end
end
local function unindex(j)
  local st = j.state
  redis.call("ZREM", k("st:" .. st), j.id)
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
          if num(c, "run_at") < now then c.run_at = s(now) end
          c.waiting_since = s(now)
          local sw = num(c, "start_within")
          if sw > 0 then c.start_by = s(now + sw) else c.start_by = "0" end
          save(c)
          index(c, now)
          released = true
        else
          save(c)
        end
      else
        unindex(c)
        c.state = "cancelled"
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
`

const insertScript = common + `
local now = tonumber(ARGV[1])
local jobs = cjson.decode(ARGV[2])
local qcfg = {}
local function queue_cfg(q)
  if qcfg[q] == nil then
    local raw = redis.call("HGET", k("cfg:queue"), q)
    if raw then qcfg[q] = cjson.decode(raw) else qcfg[q] = {} end
  end
  return qcfg[q]
end
local remap = {}
local out = {}
local any = false
for i, j in ipairs(jobs) do
  local status, rid = "ok", j.id
  local deps, failed = {}, false
  local seen = {}
  for _, d in ipairs(j.dep_ids) do
    local m = remap[d]
    if m == "" then failed = true end
    if m == nil then m = d end
    if not seen[m] then seen[m] = true; deps[#deps + 1] = m end
  end
  local waiton = {}
  if not failed then
    for _, d in ipairs(deps) do
      local st = redis.call("HGET", k("j:" .. d), "state")
      if not st or st == "dead" or st == "cancelled" then failed = true; break end
      if st ~= "completed" then waiton[#waiton + 1] = d end
    end
  end
  if failed then
    status = "dep"
    remap[j.id] = ""
  end
  if status == "ok" and j.unique_key ~= "" then
    local cur = redis.call("HGET", k("uniq"), j.unique_key)
    if cur then
      local hid, until_at = string.match(cur, "^([^|]+)|(.+)$")
      until_at = tonumber(until_at)
      local held = false
      if until_at > 0 then
        held = until_at > now
      else
        local st = redis.call("HGET", k("j:" .. hid), "state")
        held = st and not final(st)
      end
      if held then
        status, rid = "dup", hid
        remap[j.id] = hid
      end
    end
  end
  if status == "ok" then
    local cap = tonumber(queue_cfg(j.queue).max_waiting) or 0
    if cap > 0 then
      local n = tonumber(redis.call("HGET", k("cnt"), j.queue .. "|waiting") or "0") + tonumber(redis.call("HGET", k("cnt"), j.queue .. "|blocked") or "0")
      if n >= cap then
        status = "qfull"
        remap[j.id] = ""
      end
    end
  end
  if status == "ok" and j.tenant ~= "" then
    local cap = tonumber(queue_cfg(j.queue).tenant_waiting) or 0
    if cap > 0 then
      local n = tonumber(redis.call("HGET", k("wait"), j.queue .. "|" .. j.tenant) or "0")
      if n >= cap then
        status = "full"
        remap[j.id] = ""
      end
    end
  end
  if status == "ok" then
    if j.unique_key ~= "" then redis.call("HSET", k("uniq"), j.unique_key, j.id .. "|" .. j.unique_until) end
    j.pending = s(#waiton)
    j.deps = table.concat(deps, ",")
    j.dep_ids = nil
    if #waiton > 0 then
      j.state = "blocked"
      j.start_by = "0"
    else
      j.state = "waiting"
    end
    save(j)
    index(j, now)
    redis.call("ZADD", k("all"), 0, j.id)
    for _, d in ipairs(waiton) do redis.call("SADD", k("kids:" .. d), j.id) end
    any = true
  end
  out[i] = {status, rid, j.pending or "0"}
end
if any then wake() end
return out
`

const claimScript = common + `
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
`

const applyScript = common + `
local now = tonumber(ARGV[1])
local changes = cjson.decode(ARGV[2])
local MUT = {"queue","kind","state","priority","tenant","pool","attempt","max_attempts","run_at","waiting_since","start_by","start_within","worker","lease_until","updated_at","started_at","finished_at","data"}
local out = {}
for i, c in ipairs(changes) do
  local old = load(c.id)
  local ok = 0
  if old and old.version == c.expect and (tonumber(c.lease_before) == 0 or num(old, "lease_until") < tonumber(c.lease_before)) then
    local j = {}
    for _, f in ipairs(F) do j[f] = old[f] end
    for _, f in ipairs(MUT) do j[f] = c.f[f] end
    j.version = s(num(old, "version") + 1)
    unindex(old)
    save(j)
    index(j, now)
    if final(j.state) and not final(old.state) then finalize(j, now) end
    if j.state == "waiting" then wake() end
    ok = 1
  end
  out[i] = ok
end
return out
`

const extendScript = common + `
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
`

const pruneScript = common + `
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
`

const pruneEventsScript = common + `
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
`

const swapScript = common + `
local cur = redis.call("HGET", k("cursors"), ARGV[1]) or "0"
if cur == ARGV[2] then
  redis.call("HSET", k("cursors"), ARGV[1], ARGV[3])
  return 1
end
return 0
`
