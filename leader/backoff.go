package leader

import (
	"time"

	"github.com/cenkalti/backoff"
)

// NewUnlimitedExponentialBackOff returns a new exponential backoff interval
// w/o time limit
func NewUnlimitedExponentialBackOff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = 0
	b.MaxInterval = 10 * time.Second
	return b
}

func newBackoff() *countedBackoff {
	return &countedBackoff{
		b: backoff.NewExponentialBackOff(),
	}
}

func (r *countedBackoff) Reset() {
	r.steps = 0
	r.b.Reset()
}

func (r *countedBackoff) NextBackOff() time.Duration {
	return r.b.NextBackOff()
}

func (r *countedBackoff) inc() {
	r.steps += 1
}

func (r *countedBackoff) count() int {
	return r.steps
}

type countedBackoff struct {
	b     *backoff.ExponentialBackOff
	steps int
}
