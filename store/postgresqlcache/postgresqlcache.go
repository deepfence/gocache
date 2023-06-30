package postgresqlcache

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"time"

	lib_store "github.com/deepfence/gocache/lib/v4/store"
	"github.com/jackc/pgx/v5"
	"github.com/klauspost/compress/s2"
	_ "github.com/lib/pq"
	"github.com/spf13/cast"
)

const (
	// PostgresqlCacheType represents the storage type as a string value
	PostgresqlCacheType = "postgresql"
)

var (
	ttl = time.Hour * 24
)

// PostgresqlStore is a store for Redis
type PostgresqlStore struct {
	conn    *pgx.Conn
	options *lib_store.Options
}

// NewPostgresqlStore creates a new store to Redis instance(s)
// connString: postgres://username:password@localhost:5432/database_name?sslmode=disable
func NewPostgresqlStore(connString string, options ...lib_store.Option) (*PostgresqlStore, error) {
	conn, err := pgx.Connect(context.Background(), connString)
	if err != nil {
		return nil, err
	}
	postgresqlStore := PostgresqlStore{
		conn:    conn,
		options: lib_store.ApplyOptions(options...),
	}
	err = postgresqlStore.createTable(connString)
	if err != nil {
		return nil, err
	}
	return &postgresqlStore, nil
}

func (s *PostgresqlStore) createTable(connString string) error {
	connParams, err := parsePostgresqlConnectionString(connString)
	if err != nil {
		return err
	}
	dataSource := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", connParams.Host, connParams.Port, connParams.User, connParams.Password, connParams.DatabaseName, connParams.SslMode)
	db, err := sql.Open("postgres", dataSource)
	if err != nil {
		return err
	}
	err = db.Ping()
	if err != nil {
		return err
	}
	defer db.Close()
	var createTable = `
CREATE TABLE IF NOT EXISTS postgresqlcache (
	key text NOT NULL UNIQUE,
	value bytea NOT NULL
)`
	_, err = db.Exec(createTable)
	return err
}

// Close when exit store
func (s *PostgresqlStore) Close(ctx context.Context) error {
	return s.conn.Close(ctx)
}

func compress(src []byte, dst io.Writer) error {
	enc := s2.NewWriter(dst)
	// The encoder owns the buffer until Flush or Close is called.
	err := enc.EncodeBuffer(src)
	if err != nil {
		enc.Close()
		return err
	}
	// Blocks until compression is done.
	return enc.Close()
}

func decompress(src []byte) ([]byte, error) {
	bufReader := s2.NewReader(bytes.NewReader(src))
	var b bytes.Buffer
	_, err := io.Copy(&b, bufReader)
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// Get returns data stored from a given key
func (s *PostgresqlStore) Get(ctx context.Context, key any) (any, error) {
	var val []byte
	err := s.conn.QueryRow(context.Background(), "select value from postgresqlcache where key=$1", cast.ToString(key)).Scan(&val)
	if err != nil {
		return nil, err
	}
	if len(val) == 0 {
		return nil, &lib_store.NotFound{}
	}
	val, err = decompress(val)
	if err != nil {
		return nil, err
	}
	return any(val), nil
}

// GetWithTTL returns data stored from a given key and its corresponding TTL
func (s *PostgresqlStore) GetWithTTL(ctx context.Context, key any) (any, time.Duration, error) {
	val, err := s.Get(ctx, key)
	return val, ttl, err
}

func (s *PostgresqlStore) Set(ctx context.Context, key any, value any, options ...lib_store.Option) error {
	dstReader := bytes.Buffer{}
	err := compress(value.([]byte), &dstReader)
	if err != nil {
		return err
	}
	upsertQuery := `
INSERT INTO postgresqlcache (key, value)
VALUES($1, $2) 
ON CONFLICT (key) 
DO UPDATE SET value = $2`
	_, err = s.conn.Exec(context.Background(), upsertQuery, cast.ToString(key), dstReader.Bytes())
	if err != nil {
		return err
	}
	return nil
}

func (s *PostgresqlStore) Delete(ctx context.Context, key any) error {
	deleteQuery := `
DELETE FROM postgresqlcache
WHERE key = $1`
	_, err := s.conn.Exec(context.Background(), deleteQuery, cast.ToString(key))
	if err != nil {
		return err
	}
	return nil
}

func (s *PostgresqlStore) Invalidate(ctx context.Context, options ...lib_store.InvalidateOption) error {
	opts := lib_store.ApplyInvalidateOptions(options...)
	if tags := opts.Tags; len(tags) > 0 {
		for _, tag := range tags {
			result, err := s.Get(ctx, tag)
			if err != nil {
				return nil
			}

			cacheKeys := []string{}
			if bytes, ok := result.([]byte); ok {
				cacheKeys = strings.Split(string(bytes), ",")
			}

			for _, cacheKey := range cacheKeys {
				if err := s.Delete(ctx, cacheKey); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (s *PostgresqlStore) Clear(ctx context.Context) error {
	truncateQuery := `TRUNCATE TABLE postgresqlcache`
	_, err := s.conn.Exec(context.Background(), truncateQuery)
	if err != nil {
		return err
	}
	return nil
}

// GetType returns the store type
func (s *PostgresqlStore) GetType() string {
	return PostgresqlCacheType
}

type postgesqlConnectionParam struct {
	User         string
	Password     string
	Host         string
	Port         string
	DatabaseName string
	SslMode      string
}

func parsePostgresqlConnectionString(connString string) (*postgesqlConnectionParam, error) {
	u, err := url.Parse(connString)
	if err != nil {
		return nil, err
	}
	password, _ := u.User.Password()
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		return nil, err
	}
	queryParam, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return nil, err
	}
	sslMode := queryParam.Get("sslmode")
	if sslMode == "" {
		sslMode = "disable"
	}
	if port == "" {
		if sslMode == "disable" {
			port = "80"
		} else {
			port = "443"
		}
	}
	connParams := postgesqlConnectionParam{
		User:         u.User.Username(),
		Password:     password,
		Host:         host,
		Port:         port,
		DatabaseName: strings.Trim(u.Path, "/"),
		SslMode:      sslMode,
	}
	return &connParams, nil
}
