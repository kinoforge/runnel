local now = tonumber(ARGV[1])
local changes = cjson.decode(ARGV[2])
local MUT = {"queue","kind","state","priority","group","pool","attempt","max_attempts","run_at","waiting_since","start_by","start_timeout","deadline","worker","lease_until","pending","updated_at","started_at","finished_at","data"}
local out = {}
for i, c in ipairs(changes) do
  local old = load(c.id)
  local ok = 0
  if old and old.version == c.expect and (c.from == nil or c.from == "" or old.state == c.from) and (tonumber(c.lease_before) == 0 or num(old, "lease_until") < tonumber(c.lease_before)) then
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
