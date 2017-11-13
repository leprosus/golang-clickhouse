package clickhouse

import (
	"sync/atomic"
	"fmt"
	composer "github.com/leprosus/golang-composer"
)

type Limiter struct {
	maxRequests     uint32
	requestsCounter int32
}

// Sets requests limitation (zero is limitation off)
func (lim *Limiter) MaxRequests(limit int) {
	atomic.StoreUint32(&lim.maxRequests, uint32(limit))

	message := fmt.Sprintf("Set max request pool = %d", limit)
	cfg.logger.debug(message)
}

func (lim *Limiter) increaseRequests() {
	if atomic.LoadUint32(&lim.maxRequests) > 0 {
		atomic.AddInt32(&lim.requestsCounter, 1)

		if atomic.LoadUint32(&lim.maxRequests) < uint32(atomic.LoadInt32(&lim.requestsCounter)) {
			composer.GetComposer().Pause()
		}
	}
}

func (lim *Limiter) reduceRequests() {
	if atomic.LoadUint32(&lim.maxRequests) > 0 {
		atomic.AddInt32(&lim.requestsCounter, -1)

		if atomic.LoadUint32(&lim.maxRequests) >= uint32(atomic.LoadInt32(&lim.requestsCounter)) {
			composer.GetComposer().Play()
		}
	}
}

func (lim *Limiter) waitRequests() {
	composer.GetComposer().NeedWait()
}
