package pools

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

type RowEventPoolT struct {
	mu      sync.Mutex
	counter uint64
	size    uint64
	items   chan *RowEventItemT
}

type RowEventItemT struct {
	ItemID uint64 `json:"itemID"`

	Log  RowEventItemLogT  `json:"log"`
	Data RowEventItemDataT `json:"-"`
}

type RowEventItemLogT struct {
	EventType      string `json:"eventType"`
	BinlogFile     string `json:"binlogFile"`
	BinlogPosition uint64 `json:"binlogPosition"`
}

type RowEventItemDataT struct {
	Database  string           `json:"database"`
	Table     string           `json:"table"`
	Operation string           `json:"operation"`
	Rows      []map[string]any `json:"rows"`
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
		p.mu.Lock()
		p.size--
		p.mu.Unlock()
		return item, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *RowEventPoolT) Prepare(item *RowEventItemT) error {
	p.mu.Lock()
	item.ItemID = p.counter + 1
	p.mu.Unlock()

	return nil
}

func (p *RowEventPoolT) Add(ctx context.Context, item *RowEventItemT) error {
	p.mu.Lock()
	nextID := p.counter + 1
	if nextID-item.ItemID != 0 {
		p.mu.Unlock()
		return fmt.Errorf("invalid '%d' item id, next item id must be '%d'", item.ItemID, nextID)
	}
	p.counter++
	p.size++
	p.mu.Unlock()

	select {
	case p.items <- item:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *RowEventPoolT) Size() uint64 {
	p.mu.Lock()
	r := p.size
	p.mu.Unlock()
	return r
}

// ITEM POOL FUNCTIONS

func (i *RowEventItemT) String() string {
	result, err := json.Marshal(i)
	if err != nil {
		return "<nil>"
	}

	return string(result)
}
