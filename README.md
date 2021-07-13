# hookup

Hookup package establishes exchange of messages between users. It is my pet project and playground for experiments.

## Under the hood

Hookup uses kafka to store and retrieve messages. Each user has own kafka topic.

## Example

``` go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/martindrlik/hookup"
)

func main() {
	o := new(hookup.Options)
	o.Option(hookup.AcquireAttemtLimit(5))
	o.Option(hookup.Broker("localhost:9092"))
	o.Option(hookup.ConsumerLimit(5))
	o.Option(hookup.ProducerLimit(5))
	o.Option(hookup.PullLimit(15))
	o.Option(hookup.PullLinger(time.Second))
	h := o.Setup()
	h.RegisterUsers([]string{"foo", "bar"})
	err := h.PostMessage(context.TODO(), "Hello", "foo", "bar")
	fatal(err)
	m, err := h.PullMessages(context.TODO(), "bar")
	fatal(err)
	fmt.Println(m) // prints [message: Hello, from: foo]
}

func fatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
```
