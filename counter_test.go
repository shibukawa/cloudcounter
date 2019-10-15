package cloudcounter

import (
	"context"
	"github.com/rs/xid"
	"github.com/stretchr/testify/assert"
	"gocloud.dev/docstore"
	_ "gocloud.dev/docstore/memdocstore"
	"golang.org/x/sync/errgroup"
	"testing"
)

func TestIncrement(t *testing.T) {
	coll, err := docstore.OpenCollection(context.Background(), "mem://counter"+xid.New().String()+"/id")
	assert.Nil(t, err)
	if err != nil {
		return
	}
	var testKey CounterKey = "test"

	counter := NewCounter(coll)
	err = counter.Register(context.Background(), testKey)
	assert.Nil(t, err)
	count, err := counter.Get(context.Background(), testKey)
	assert.Nil(t, err)
	assert.Equal(t, 0, count)

	eg := errgroup.Group{}

	for i := 0; i < 100; i++ {
		eg.Go(func() error {
			counter.Increment(context.Background(), testKey)
			return nil
		})
	}
	eg.Wait()

	count, err = counter.Get(context.Background(), testKey)
	assert.Nil(t, err)
	assert.Equal(t, 100, count)
}
