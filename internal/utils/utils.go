package utils

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"strings"

	"binwatch/api/v1alpha2"
	"binwatch/internal/logger"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	mysqldrv "github.com/go-sql-driver/mysql"
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

func IsRegisteredPort(port uint32) bool {
	// Usable ports: 1024 - 49151
	return port >= 1024 && port <= 49151
}

func IsDynamicPort(port uint32) bool {
	// Usable ports: 49152 - 65535
	return port >= 49152 && port <= 65535
}

// GetBasicLogExtraFields TODO
func GetBasicLogExtraFields(componentName string) logger.ExtraFieldsT {
	return logger.ExtraFieldsT{
		"service":   "BinWatch",
		"component": componentName,
	}
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

// GetTLSConfig builds the TLS configuration used to connect to the source
// database: the CA verifies the server certificate and Cert/Key present a
// client certificate (mTLS). ServerName is required by the TLS handshake
// when verification is enabled; it defaults to the source host unless
// overridden in the config. Returns nil when TLS is disabled.
func GetTLSConfig(cfg v1alpha2.SourceTLST, defaultServerName string) (*tls.Config, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	serverName := cfg.ServerName
	if serverName == "" {
		serverName = defaultServerName
	}

	tlsCfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         serverName,
		InsecureSkipVerify: cfg.InsecureSkipVerify,
	}

	if cfg.CA != "" {
		caPEM, err := os.ReadFile(cfg.CA)
		if err != nil {
			return nil, fmt.Errorf("unable to read TLS CA file %q: %w", cfg.CA, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("unable to parse any certificate from TLS CA file %q", cfg.CA)
		}
		tlsCfg.RootCAs = pool

		// CloudSQL server certificates do not include the instance IP in the
		// SANs, so standard hostname verification fails. With
		// insecureSkipVerify enabled the chain is still verified against the
		// CA here, only the hostname check is skipped.
		if cfg.InsecureSkipVerify {
			tlsCfg.VerifyPeerCertificate = verifyPeerCertAgainstCA(pool)
		}
	}

	if cfg.Cert != "" || cfg.Key != "" {
		cert, err := tls.LoadX509KeyPair(cfg.Cert, cfg.Key)
		if err != nil {
			return nil, fmt.Errorf("unable to load TLS client keypair (cert %q, key %q): %w", cfg.Cert, cfg.Key, err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return tlsCfg, nil
}

func verifyPeerCertAgainstCA(pool *x509.CertPool) func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return fmt.Errorf("no server certificate presented")
		}
		certs := make([]*x509.Certificate, 0, len(rawCerts))
		for _, raw := range rawCerts {
			cert, err := x509.ParseCertificate(raw)
			if err != nil {
				return fmt.Errorf("unable to parse server certificate: %w", err)
			}
			certs = append(certs, cert)
		}
		opts := x509.VerifyOptions{
			Roots:         pool,
			Intermediates: x509.NewCertPool(),
		}
		for _, cert := range certs[1:] {
			opts.Intermediates.AddCert(cert)
		}
		_, err := certs[0].Verify(opts)
		return err
	}
}

type DBOptions struct {
	Flavor    string
	User      string
	Pass      string
	Host      string
	Port      uint32
	TLSConfig *tls.Config
}

// GetTableColumns TODO
func GetTableColumns(ops DBOptions, dbTables map[string][]string) (dbTableColsNames map[string][]string, err error) {
	dsn := fmt.Sprintf("%s:%s@(%s:%d)/", ops.User, ops.Pass, ops.Host, ops.Port)
	if ops.TLSConfig != nil {
		err = mysqldrv.RegisterTLSConfig("binwatch", ops.TLSConfig)
		if err != nil {
			return dbTableColsNames, err
		}
		dsn += "?tls=binwatch"
	}
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

			columns, err := rows.Columns()
			if err != nil {
				rows.Close()
				return dbTableColsNames, err
			}
			rows.Close()
			
			currentKey := strings.Join([]string{dbk, tv}, ".")
			dbTableColsNames[currentKey] = append(dbTableColsNames[currentKey], columns...)
		}
	}

	return dbTableColsNames, err
}
