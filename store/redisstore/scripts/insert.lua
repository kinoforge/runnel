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
