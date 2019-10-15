package cloudcounter

import (
	"context"
	"fmt"
	"gocloud.dev/docstore"
	"log"
	"math/rand"
)

type CounterKey string

func counterID(key CounterKey, prefix string, i int) string {
	return fmt.Sprintf("%s%s%x", prefix, string(key), i)
}

type Counter struct {
	collection   *docstore.Collection
	concurrency  int
	prefix       string
	counterNames map[CounterKey]bool
	counterKeys  map[CounterKey][]string
}

type CounterEntity struct {
	ID    string `docstore:"id"`
	Count int    `docstore:"count"`
}

type Option struct {
	Concurrency int
	Prefix      string
}

func NewCounter(collection *docstore.Collection, opt ...Option) *Counter {
	var o Option
	if len(opt) > 0 {
		o = opt[0]
	}
	if o.Concurrency == 0 {
		o.Concurrency = 10
	}
	return &Counter{
		collection:   collection,
		concurrency:  o.Concurrency,
		prefix:       o.Prefix,
		counterNames: make(map[CounterKey]bool),
		counterKeys:  make(map[CounterKey][]string),
	}
}

func (c *Counter) Register(ctx context.Context, key CounterKey) error {
	c.counterNames[key] = true
	c.counterKeys[key] = make([]string, c.concurrency)
	for i := 0; i < c.concurrency; i++ {
		id := counterID(key, c.prefix, i)
		record := &CounterEntity{
			ID:    id,
			Count: 0,
		}
		c.counterKeys[key][i] = id
		err := c.collection.Create(ctx, record)
		if err != nil {
			log.Println(err)
		}
	}
	return nil
}

func (c *Counter) Increment(ctx context.Context, key CounterKey) (int, error) {
	index := rand.Intn(c.concurrency)
	update := CounterEntity{
		ID: c.counterKeys[key][index],
	}
	actions := c.collection.Actions().
		Update(&update, docstore.Mods{"count": docstore.Increment(1)})
	return c.getTotalValue(ctx, key, actions)
}

func (c *Counter) getTotalValue(ctx context.Context, key CounterKey, actions *docstore.ActionList) (int, error) {
	values := make([]CounterEntity, c.concurrency)
	for i := 0; i < c.concurrency; i++ {
		values[i].ID = c.counterKeys[key][i]
		actions = actions.Get(&values[i])
	}
	err := actions.Do(ctx)
	if err != nil {
		return 0, err
	}
	var total int
	for _, value := range values {
		total += value.Count
	}
	return total, nil
}

func (c *Counter) Decrement(ctx context.Context, key CounterKey) error {
	index := rand.Intn(c.concurrency)
	update := CounterEntity{
		ID: c.counterKeys[key][index],
	}
	return c.collection.Update(ctx, &update, docstore.Mods{"count": docstore.Increment(-1)})
}

func (c *Counter) Get(ctx context.Context, key CounterKey) (int, error) {
	return c.getTotalValue(ctx, key, c.collection.Actions())
}
