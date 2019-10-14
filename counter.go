package cloudcounter

import (
	"context"
	"log"
	"math/rand"
	"strconv"

	"gocloud.dev/docstore"
)

type CounterKey string

func counterID(key CounterKey, i int) string {
	return "c" + string(key) + strconv.FormatInt(int64(i), 16)
}

type Counter struct {
	collection   *docstore.Collection
	concurrency  int
	counterNames map[CounterKey]bool
}

type CounterEntity struct {
	ID    string `docstore:"id"`
	Count int    `docstore:"count"`
}

func NewCounter(collection *docstore.Collection, concurrency int) *Counter {
	return &Counter{
		collection:   collection,
		concurrency:  concurrency,
		counterNames: make(map[CounterKey]bool),
	}
}

func (c *Counter) Register(ctx context.Context, key CounterKey) error {
	c.counterNames[key] = true
	for i := 0; i < c.concurrency; i++ {
		record := &CounterEntity{
			ID:    counterID(key, i),
			Count: 0,
		}
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
		ID: counterID(key, index),
	}
	actions := c.collection.Actions().
		Update(&update, docstore.Mods{"count": docstore.Increment(1)})
	return c.getTotalValue(ctx, key, actions)
}

func (c *Counter) getTotalValue(ctx context.Context, key CounterKey, actions *docstore.ActionList) (int, error) {
	values := make([]CounterEntity, c.concurrency)
	for i := 0; i < c.concurrency; i++ {
		values[i].ID = counterID(key, i)
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
		ID: counterID(key, index),
	}
	return c.collection.Update(ctx, &update, docstore.Mods{"count": docstore.Increment(-1)})
}

func (c *Counter) Get(ctx context.Context, key CounterKey) (int, error) {
	return c.getTotalValue(ctx, key, c.collection.Actions())
}
