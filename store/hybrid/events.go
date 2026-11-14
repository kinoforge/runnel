package hybrid

import (
	"cmp"

	"github.com/kinoforge/runnel"
)

type event struct {
	seq, at     int64
	id          uid
	queue, kind sym
	state       uint8
}

func (sp *space) addEvent(e event) {
	sp.events = append(sp.events, e)
	sp.seq = max(sp.seq, e.seq)
}

func (sp *space) event(e *event) runnel.Event {
	return runnel.Event{Seq: e.seq, Namespace: sp.ns, JobID: e.id.String(), Queue: sp.syms.name(e.queue),
		Kind: sp.syms.name(e.kind), State: stateNames[e.state], At: fromMs(e.at)}
}

func (sp *space) dropEvents(seq int64) {
	i := 0
	for i < len(sp.events) && sp.events[i].seq <= seq {
		i++
	}
	sp.events = sp.events[i:]
}

func cmpEvent(e event, seq int64) int { return cmp.Compare(e.seq, seq) }
