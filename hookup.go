package hookup

type Hookup struct {
	consumerPool *ConsumerPool
	producerPool *producerPool
	options      *Options
}

func newHookup(o *Options) *Hookup {
	cp := newConsumerPool(o.broker, o.consumerLimit)
	pp := newProducerPool(o.broker, o.producerLimit)
	return &Hookup{cp, pp, o}
}
