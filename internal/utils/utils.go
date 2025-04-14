package utils

import (
	"binwatch/internal/logger"
	"database/sql"
	"os"
	"regexp"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// ExpandEnv TODO
func ExpandEnv(input []byte) []byte {
	re := regexp.MustCompile(`\${ENV:([A-Za-z_][A-Za-z0-9_]*)}\$`)
	result := re.ReplaceAllFunc(input, func(match []byte) []byte {
		key := re.FindSubmatch(match)[1]
		if value, exists := os.LookupEnv(string(key)); exists {
			return []byte(value)
		}
		return match
	})

	return result
}

// GetBasicLogExtraFields TODO
func GetBasicLogExtraFields(componentName string) logger.ExtraFieldsT {
	return logger.ExtraFieldsT{
		"service":   "BinWatch",
		"component": componentName,
	}
}

// GetCurrentBinlogPos TODO
func GetCurrentBinlogPos(dsn string) (mysql.Position, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return mysql.Position{}, err
	}
	defer db.Close()

	var (
		file string
		pos  uint32
	)

	row := db.QueryRow("SHOW MASTER STATUS")
	err = row.Scan(&file, &pos, new(string), new(string), new(string)) // ignoramos columnas extra
	if err != nil {
		return mysql.Position{}, err
	}

	return mysql.Position{
		Name: file,
		Pos:  pos,
	}, nil
}

// GetMysqlActionFromRowsEventType TODO
func GetMysqlActionFromRowsEventType(et replication.EventType) (eType string) {
	switch et {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		{
			return "insert"
		}
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		{
			return "update"
		}
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		{
			return "delete"
		}
	}
	return ""
}
