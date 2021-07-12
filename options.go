package hookup

import "time"

type Options struct {
	acquireAttemptLimit int
	broker              string
	consumerLimit       int
	producerLimit       int
	pullLimit           int
	pullLinger          time.Duration
}

// Note Rob Pike's Self-referential functions and the design of options

type option func(o *Options) option

// Option sets the options specified.
// It returns an option to restore the last arg's previous value.
func (o *Options) Option(opts ...option) (previous option) {
	for _, opt := range opts {
		previous = opt(o)
	}
	return previous
}

func (o *Options) Setup() *Hookup {
	return newHookup(o)
}

// AcquireAttemptLimit sets maximum number of attempts to acquire consumer or producer from pool to v.
func AcquireAttemtLimit(v int) option {
	return func(o *Options) option {
		previous := o.acquireAttemptLimit
		o.acquireAttemptLimit = v
		return AcquireAttemtLimit(previous)
	}
}

// Broker sets kafka broker to v.
func Broker(v string) option {
	return func(o *Options) option {
		previous := o.broker
		o.broker = v
		return Broker(previous)
	}
}

// ConsumerLimit sets consumer pool limit to v.
func ConsumerLimit(v int) option {
	return func(o *Options) option {
		previous := o.consumerLimit
		o.consumerLimit = v
		return ConsumerLimit(previous)
	}
}

// ProducerLimit sets producer pool limit to v.
func ProducerLimit(v int) option {
	return func(o *Options) option {
		previous := o.producerLimit
		o.producerLimit = v
		return ProducerLimit(previous)
	}
}

// PullLimit sets pull messages limit to v.
func PullLimit(v int) option {
	return func(o *Options) option {
		previous := o.pullLimit
		o.pullLimit = v
		return PullLimit(previous)
	}
}

// PullLinger sets pull messages limit to v.
func PullLinger(v time.Duration) option {
	return func(o *Options) option {
		previous := o.pullLinger
		o.pullLinger = v
		return PullLinger(previous)
	}
}
