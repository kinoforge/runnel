package runnel

func (w *Worker) Concurrency() int { return int(w.slots.Load()) }

func (w *Worker) SetConcurrency(n int) error {
	if n < 1 {
		return invalid("concurrency must be at least 1")
	}
	w.slots.Store(int64(n))
	w.mu.Lock()
	freed := w.freed
	w.mu.Unlock()
	if freed != nil {
		select {
		case freed <- struct{}{}:
		default:
		}
	}
	return nil
}
