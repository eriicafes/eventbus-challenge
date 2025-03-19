package main

import (
	"fmt"
	"os"
	"sync"
	"text/tabwriter"
	"time"
)

type Priority string

const (
	PriorityLow  Priority = "low"
	PriorityHigh Priority = "high"
)

type Subscriber[T any] = func(data T)

type EventBus struct{ ch chan func() }

func NewEventBus() *EventBus {
	ch := make(chan func())
	go func() {
		// execute notifiers serially
		for fn := range ch {
			fn()
		}
	}()
	return &EventBus{ch: ch}
}

func (e *EventBus) Run(fn func()) {
	waitCh := make(chan struct{})
	e.ch <- func() {
		fn()
		close(waitCh)
	}
	<-waitCh
}

type Event[T any] struct {
	bus         *EventBus
	priority    Priority
	subscribers []Subscriber[T]
}

func NewEvent[T any](bus *EventBus, priority Priority) *Event[T] {
	return &Event[T]{
		bus:      bus,
		priority: priority,
	}
}

func (e *Event[T]) Subscribe(fn Subscriber[T]) {
	e.subscribers = append(e.subscribers, fn)
}

func (e *Event[T]) Emit(data T) {
	switch e.priority {
	case PriorityHigh:
		// run high priority immediately
		e.run(e.subscribers, data)
	case PriorityLow:
		// run low priority in batches of 20
		var batch []Subscriber[T]
		for _, subscriber := range e.subscribers {
			batch = append(batch, subscriber)
			if len(batch) >= 20 {
				e.run(batch, data)
				batch = nil // reset batch
			}
		}
		// run remaining items in batch if any
		if len(batch) > 0 {
			e.run(batch, data)
		}
	}
}

func (e *Event[T]) run(subscribers []Subscriber[T], data T) {
	e.bus.Run(func() {
		for _, subscriber := range subscribers {
			subscriber(data)
		}
	})
}

func main() {
	eventBus := NewEventBus()
	event1 := NewEvent[string](eventBus, PriorityHigh)
	event2 := NewEvent[string](eventBus, PriorityLow)

	start := time.Now()
	logger := func(event, subscriber string) {
		diff := time.Since(start)
		w := new(tabwriter.Writer).Init(os.Stdout, 8, 2, 2, ' ', 0) // format tab spacing
		fmt.Fprintf(w, "%ds:\t %s\t %s\n", int(diff.Seconds()), event, subscriber)
		w.Flush()                   // flush print output
		time.Sleep(time.Second * 1) // 1 second delay
	}

	// add event1 subscribers
	for i := range 10 {
		event1.Subscribe(func(data string) {
			logger(data, fmt.Sprintf("subscriber%d", i+1))
		})
	}
	// add event2 subscribers
	for i := range 100 {
		event2.Subscribe(func(data string) {
			logger(data, fmt.Sprintf("subscriber%d", i+1))
		})
	}

	var wg sync.WaitGroup
	wg.Add(4)
	go func() {
		event2.Emit("event2")
		wg.Done()
	}()
	go func() {
		time.Sleep(time.Second * 10) // 10 seconds mark
		event1.Emit("event1 (t=10)")
		wg.Done()
	}()
	go func() {
		time.Sleep(time.Second * 20) // 20 seconds mark
		event1.Emit("event1 (t=20)")
		wg.Done()
	}()
	go func() {
		time.Sleep(time.Second * 50) // 50 seconds mark
		event1.Emit("event1 (t=50)")
		wg.Done()
	}()
	wg.Wait()
}
