package pools

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

type RowEventPoolT struct {
	mu      sync.Mutex
	items   chan *RowEventItemT
	counter int
}

type RowEventItemT struct {
	PoolID int `json:"poolID"`

	EventType string           `json:"eventType"`
	Location  RowEventItemPosT `json:"location"`

	Database string  `json:"database"`
	Table    string  `json:"table"`
	Action   string  `json:"action"`
	Rows     [][]any `json:"rows"`
}

type RowEventItemPosT struct {
	File     string `json:"file"`
	Position uint64 `json:"position"`
}

// POOL FUNCTIONS

func NewRowEventPool(poolSize int) *RowEventPoolT {
	return &RowEventPoolT{
		items: make(chan *RowEventItemT, poolSize),
	}
}

func (p *RowEventPoolT) Get(ctx context.Context) (*RowEventItemT, error) {
	select {
	case item := <-p.items:
		return item, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *RowEventPoolT) Prepare(item *RowEventItemT) error {
	p.mu.Lock()
	item.PoolID = p.counter + 1
	p.mu.Unlock()

	return nil
}

func (p *RowEventPoolT) Add(ctx context.Context, item *RowEventItemT) error {
	p.mu.Lock()
	nextID := p.counter + 1
	if nextID-item.PoolID != 0 {
		p.mu.Unlock()
		return fmt.Errorf("invalid '%d' item id, next item id must be '%d'", item.PoolID, nextID)
	}
	p.counter++
	p.mu.Unlock()

	select {
	case p.items <- item:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ITEM POOL FUNCTIONS

func (i *RowEventItemT) String() string {
	result, err := json.Marshal(i)
	if err != nil {
		return "<nil>"
	}

	return string(result)
}
