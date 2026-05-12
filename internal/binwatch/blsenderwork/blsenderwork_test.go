package blsenderwork

import (
	"testing"

	"binwatch/internal/logger"
	"binwatch/internal/pools"
	"binwatch/internal/tmpl"
)

func newTestWorker(t *testing.T, count, index uint64, keyTmplStr string) *BLSenderWorkT {
	t.Helper()
	w := &BLSenderWorkT{
		log:          logger.NewLogger(logger.GetLevel("error")),
		shardEnabled: true,
		shardCount:   count,
		shardIndex:   index,
	}
	if keyTmplStr != "" {
		tpl, err := tmpl.NewTemplate("test-key", keyTmplStr)
		if err != nil {
			t.Fatalf("unexpected template parse error: %v", err)
		}
		w.shardKeyTmpl = tpl
	}
	return w
}

func itemAtPos(pos uint64) *pools.RowEventItemT {
	return &pools.RowEventItemT{Log: pools.RowEventItemLogT{BinlogPosition: pos}}
}

func itemWithID(id any) *pools.RowEventItemT {
	return &pools.RowEventItemT{
		Data: pools.RowEventItemDataT{
			Rows: []map[string]any{{"id": id}},
		},
	}
}

// Sharding disabled: every event is processed.
func TestShouldProcess_DisabledAcceptsAll(t *testing.T) {
	w := &BLSenderWorkT{shardEnabled: false}
	for pos := uint64(0); pos < 100; pos++ {
		if !w.shouldProcess(itemAtPos(pos)) {
			t.Fatalf("expected event at pos %d to be processed when sharding disabled", pos)
		}
	}
}

// BinlogPosition fallback: each event is routed to exactly one shard, and
// the load spreads roughly evenly across shards once it's hashed.
func TestShouldProcess_BinlogPositionPartitionsAllEvents(t *testing.T) {
	const count uint64 = 3
	workers := []*BLSenderWorkT{
		newTestWorker(t, count, 0, ""),
		newTestWorker(t, count, 1, ""),
		newTestWorker(t, count, 2, ""),
	}

	counts := make([]int, count)
	const total = 3000
	for pos := uint64(1); pos <= total; pos++ {
		matches := 0
		for idx, w := range workers {
			if w.shouldProcess(itemAtPos(pos)) {
				counts[idx]++
				matches++
			}
		}
		if matches != 1 {
			t.Fatalf("event at pos %d matched %d shards, want exactly 1", pos, matches)
		}
	}

	expected := total / int(count)
	tolerance := expected / 5 // 20% skew tolerance after FNV-1a hashing
	for idx, c := range counts {
		if c < expected-tolerance || c > expected+tolerance {
			t.Errorf("shard %d processed %d events, want ~%d (±%d)", idx, c, expected, tolerance)
		}
	}
}

// Regression test: BinlogPosition is a byte offset in the binlog file, so
// consecutive events advance by similar deltas. With raw `pos % count` and
// an even step (e.g. 150-byte events with count=2), every event would land
// on the same shard. Hashing through FNV-1a before the modulo must keep the
// distribution balanced.
func TestShouldProcess_BinlogPositionByteOffsetEvenStep(t *testing.T) {
	const count uint64 = 2
	workers := []*BLSenderWorkT{
		newTestWorker(t, count, 0, ""),
		newTestWorker(t, count, 1, ""),
	}

	counts := make([]int, count)
	const total = 2000
	const step uint64 = 150
	for i := uint64(1); i <= total; i++ {
		pos := i * step
		matches := 0
		for idx, w := range workers {
			if w.shouldProcess(itemAtPos(pos)) {
				counts[idx]++
				matches++
			}
		}
		if matches != 1 {
			t.Fatalf("event at pos %d matched %d shards, want exactly 1", pos, matches)
		}
	}

	expected := total / int(count)
	tolerance := expected / 5
	for idx, c := range counts {
		if c < expected-tolerance || c > expected+tolerance {
			t.Errorf("shard %d processed %d events with %d-byte step positions, want ~%d (±%d)", idx, c, step, expected, tolerance)
		}
	}
}

// KeyTemplate-based hashing: same key always lands on the same shard
// regardless of BinlogPosition.
func TestShouldProcess_KeyTemplateRowAffinity(t *testing.T) {
	const count uint64 = 3
	const keyTmpl = `{{ (index .Data.Rows 0).id }}`

	workers := []*BLSenderWorkT{
		newTestWorker(t, count, 0, keyTmpl),
		newTestWorker(t, count, 1, keyTmpl),
		newTestWorker(t, count, 2, keyTmpl),
	}

	// For a given id, find which shard owns it on the first call,
	// then verify it's stable across many subsequent calls.
	for _, id := range []int{1, 42, 99, 12345, 7777777} {
		var owner = -1
		for idx, w := range workers {
			if w.shouldProcess(itemWithID(id)) {
				if owner != -1 {
					t.Fatalf("id %d matched multiple shards (%d and %d)", id, owner, idx)
				}
				owner = idx
			}
		}
		if owner == -1 {
			t.Fatalf("id %d matched no shard", id)
		}

		// 50 repeats with same id should all hit the same shard.
		for i := 0; i < 50; i++ {
			if !workers[owner].shouldProcess(itemWithID(id)) {
				t.Fatalf("id %d should consistently route to shard %d", id, owner)
			}
		}
	}
}

// KeyTemplate distributes across all shards over a wide id space.
func TestShouldProcess_KeyTemplateSpreadsLoad(t *testing.T) {
	const count uint64 = 3
	const keyTmpl = `{{ (index .Data.Rows 0).id }}`

	workers := []*BLSenderWorkT{
		newTestWorker(t, count, 0, keyTmpl),
		newTestWorker(t, count, 1, keyTmpl),
		newTestWorker(t, count, 2, keyTmpl),
	}

	counts := make([]int, count)
	const total = 3000
	for id := 1; id <= total; id++ {
		for idx, w := range workers {
			if w.shouldProcess(itemWithID(id)) {
				counts[idx]++
			}
		}
	}

	// Expect roughly total/count per shard; allow 20% skew (FNV-1a on small
	// integers is not uniform but is close enough at this scale).
	expected := total / int(count)
	tolerance := expected / 5
	for idx, c := range counts {
		if c < expected-tolerance || c > expected+tolerance {
			t.Errorf("shard %d got %d events, want ~%d (±%d)", idx, c, expected, tolerance)
		}
	}
}