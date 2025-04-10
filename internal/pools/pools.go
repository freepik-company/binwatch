package pools

import (
	"encoding/json"
	"maps"
	"strings"
	"sync"
)

type ConnectorsQueuePoolT struct {
	mu    sync.RWMutex
	queue map[string]QueueItems
	size  int
}

type QueueItems struct {
	EventType     string `json:"eventType"`
	EventTable    string `json:"eventTable"`
	EventDatabase string `json:"eventDatabase"`
	Data          []byte `json:"data"`
}

func NewConnectorsQueuePool() *ConnectorsQueuePoolT {
	return &ConnectorsQueuePoolT{
		queue: map[string]QueueItems{},
	}
}

// SERVER POOL FUNCTIONS

func (p *ConnectorsQueuePoolT) GetCopy() (result map[string]QueueItems) {
	result = map[string]QueueItems{}

	p.mu.Lock()
	maps.Copy(result, p.queue)
	p.mu.Unlock()

	return result
}

func (p *ConnectorsQueuePoolT) Add(queue QueueItems) {
	p.mu.Lock()
	p.queue[queue.GetKey()] = queue
	p.size++
	p.mu.Unlock()
}

func (p *ConnectorsQueuePoolT) Size() (s int) {
	p.mu.Lock()
	s = p.size
	p.mu.Unlock()
	return s
}

func (p *ConnectorsQueuePoolT) Remove(key string) {
	p.mu.Lock()
	if p.size == 0 {
		p.mu.Unlock()
		return
	}

	delete(p.queue, key)
	p.size--
	p.mu.Unlock()
}

func (p *ConnectorsQueuePoolT) RemoveQueues(queues []QueueItems) {
	p.mu.Lock()
	for _, q := range queues {
		delete(p.queue, q.GetKey())
	}
	p.mu.Unlock()
}

func (q *QueueItems) String() string {
	result, err := json.Marshal(q)
	if err != nil {
		return "<nil>"
	}

	return string(result)
}

func (q *QueueItems) GetKey() string {
	return strings.Join([]string{q.EventDatabase, q.EventTable, q.EventType}, ".")
}
