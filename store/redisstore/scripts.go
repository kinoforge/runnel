package redisstore

import (
	_ "embed"

	"github.com/redis/go-redis/v9"
)

var (
	//go:embed scripts/common.lua
	common string
	//go:embed scripts/insert.lua
	insertLua string
	//go:embed scripts/claim.lua
	claimLua string
	//go:embed scripts/apply.lua
	applyLua string
	//go:embed scripts/extend.lua
	extendLua string
	//go:embed scripts/prune.lua
	pruneLua string
	//go:embed scripts/prune_events.lua
	pruneEventsLua string
	//go:embed scripts/swap.lua
	swapLua string
	//go:embed scripts/swap_config.lua
	swapConfigLua string
)

type scripts struct {
	insert, claim, apply, extend, prune, pruneEvents, swap, swapConfig *redis.Script
}

func newScripts() scripts {
	load := func(body string) *redis.Script { return redis.NewScript(common + body) }
	return scripts{
		insert: load(insertLua), claim: load(claimLua), apply: load(applyLua), extend: load(extendLua),
		prune: load(pruneLua), pruneEvents: load(pruneEventsLua), swap: load(swapLua),
		swapConfig: load(swapConfigLua),
	}
}
