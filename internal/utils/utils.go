package utils

import (
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"strings"

	"binwatch/internal/logger"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"
)

const (
	FileMode os.FileMode = 0644
	DirMode  os.FileMode = 0744

	DMLOperationInsert = "INSERT"
	DMLOperationUpdate = "UPDATE"
	DMLOperationDelete = "DELETE"
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

// GetCurrentBinlogLocation TODO
func GetCurrentBinlogLocation(canalCfg *canal.Config) (loc mysql.Position, err error) {
	var ctmp *canal.Canal
	ctmp, err = canal.NewCanal(canalCfg)
	if err != nil {
		return loc, err
	}
	defer ctmp.Close()

	loc, err = ctmp.GetMasterPos()

	return loc, err
}

// GetDMLOperationFromRowsEventType TODO
func GetDMLOperationFromRowsEventType(et replication.EventType) (eType string) {
	switch et {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		{
			return DMLOperationInsert
		}
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		{
			return DMLOperationUpdate
		}
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		{
			return DMLOperationDelete
		}
	}
	return ""
}

type DBOptions struct {
	Flavor string
	User   string
	Pass   string
	Host   string
	Port   uint32
}

// GetTableColumns TODO
func GetTableColumns(ops DBOptions, dbTables map[string][]string) (dbTableColsNames map[string][]string, err error) {
	dsn := fmt.Sprintf("%s:%s@(%s:%d)/", ops.User, ops.Pass, ops.Host, ops.Port)
	db, err := sql.Open(ops.Flavor, dsn)
	if err != nil {
		return dbTableColsNames, err
	}
	defer db.Close()

	dbTableColsNames = make(map[string][]string)
	for dbk, dbv := range dbTables {
		for _, tv := range dbv {
			query := fmt.Sprintf("SELECT * FROM `%s`.`%s` LIMIT 0", dbk, tv)
			rows, err := db.Query(query)
			if err != nil {
				return dbTableColsNames, err
			}
			defer rows.Close()

			columns, err := rows.Columns()
			if err != nil {
				return dbTableColsNames, err
			}
			currentKey := strings.Join([]string{dbk, tv}, ".")
			dbTableColsNames[currentKey] = append(dbTableColsNames[currentKey], columns...)
		}
	}

	return dbTableColsNames, err
}
