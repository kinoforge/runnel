local cur = redis.call("HGET", k("cursors"), ARGV[1]) or "0"
if cur == ARGV[2] then
  redis.call("HSET", k("cursors"), ARGV[1], ARGV[3])
  return 1
end
return 0
