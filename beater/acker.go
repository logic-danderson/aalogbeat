package beater

import (
	"context"
	"sync"

	"github.com/elastic/beats/libbeat/common/atomic"

	"github.com/logic-danderson/aalogbeat/checkpoint"
)

type eventACKer struct {
	active 		*atomic.Int
	wg 			*sync.WaitGroup
	checkpoint 	*checkpoint.Checkpoint
}

func newEventACKer(checkpoint *checkpoint.Checkpoint) *eventACKer {
	return &eventACKer{
		active: 		atomic.NewInt(0),
		wg: 				&sync.WaitGroup{},
		checkpoint: checkpoint,
	}
}

// ACKEvents receives callbacks from the publisher for every event that is 
// published. It persist the record number of the last event in each
func (a *eventACKer) ACKEvents(data []interface{}) {
	states := make(map[string]*checkpoint.AaLogState)

	for _, datum := range data {
		if st, ok := datum.(checkpoint.AaLogState); ok {
			states[st.FileName] = &st
		}
	}

	for _, st := range states {
		a.checkpoint.PersistState(*st)
	}

	// Mark events as done (subtract)
	a.active.Add(-1 * len(data))
	a.wg.Add(-1 * len(data))
}

// Wait waits for all events to be ACKed or for the context to be done
func (a *eventACKer) Wait(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		defer cancel()
		a.wg.Wait()
	}()
	<-ctx.Done()
}

// Add adds to the number of active events
func (a *eventACKer) Add(delta int) {
	a.active.Add(delta)
	a.wg.Add(delta)
}

// Active returns the number of active events (published but not yet ACKed)
func (a *eventACKer) Active() int {
	return a.active.Load()
}