package blreaderwork

import (
	"context"
	"slices"

	"binwatch/internal/pools"
	"binwatch/internal/utils"

	"github.com/go-mysql-org/go-mysql/replication"
)

func (w *BLReaderWorkT) processEvent(ctx context.Context, e *replication.BinlogEvent) (err error) {
	var extra = utils.GetBasicLogExtraFields(componentName)

	switch e.Event.(type) {
	case *replication.RotateEvent:
		{
			re := e.Event.(*replication.RotateEvent)

			w.mysql.blPos.Name = string(re.NextLogName)
			w.mysql.blPos.Pos = uint32(re.Position)

			extra.Set("location", pools.RowEventItemPosT{
				File:     w.mysql.blPos.Name,
				Position: re.Position,
			})

			w.log.Debug("rotate binlog file", extra)
		}
	case *replication.RowsEvent:
		{
			re := e.Event.(*replication.RowsEvent)

			if w.mysql.database != string(re.Table.Schema) {
				return
			}
			if !slices.Contains(w.mysql.tables, string(re.Table.Table)) {
				return
			}

			item := &pools.RowEventItemT{
				EventType: e.Header.EventType.String(),
				Location: pools.RowEventItemPosT{
					File:     w.mysql.blPos.Name,
					Position: uint64(e.Header.LogPos),
				},

				Database: string(re.Table.Schema),
				Table:    string(re.Table.Table),
				Action:   utils.GetMysqlActionFromRowsEventType(e.Header.EventType),
			}
			rowi := 0
			for ri := range re.Rows {
				if item.Action == "update" && ri%2 == 0 {
					continue
				}
				item.Rows = append(item.Rows, []any{})
				item.Rows[rowi] = append(item.Rows[rowi], re.Rows[ri]...)
				rowi++
			}
			w.rePool.Prepare(item)

			extra.Set("currentItem", item)
			w.log.Debug("add item to pool", extra)

			w.rePool.Add(ctx, item)
		}
	default:
		{
			// fmt.Printf("DEFAULT --------------------------------------------------------\n")
			// repr.Println(e)
			// // fmt.Printf("Event type: '%s'\n", utils.GetMysqlActionFromRowsEventType(e.Header.EventType))
			// // fmt.Printf("Log Position: '%d'\n", e.Header.LogPos)
			// fmt.Printf("----------------------------------------------------------------\n")
		}
	}

	return err
}
