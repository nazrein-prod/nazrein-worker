package db

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func ConnectClickhouse() (driver.Conn, error) {
	ctx := context.Background()
	var conn driver.Conn
	var err error

	url := os.Getenv("CLICKHOUSE_URL")
	dbName := os.Getenv("CLICKHOUSE_DATABASE")
	username := os.Getenv("CLICKHOUSE_USERNAME")
	password := os.Getenv("CLICKHOUSE_PASSWORD")

	for i := 1; i <= 10; i++ {
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{url},
			Auth: clickhouse.Auth{
				Database: dbName,
				Username: username,
				Password: password,
			},
			ClientInfo: clickhouse.ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{Name: "nazrein-clickhouse-api-server", Version: "1.0"},
				},
			},
			Debugf: func(format string, v ...interface{}) {
				fmt.Printf(format, v)
			},
		})

		if err == nil {
			err = conn.Ping(ctx)
			if err == nil {
				fmt.Println("Connected to ClickHouse...")
				return conn, nil
			}
		}

		fmt.Printf("Attempt %d: ClickHouse not ready: %v\n", i, err)
		time.Sleep(10 * time.Second)
	}

	return nil, fmt.Errorf("could not connect to ClickHouse after multiple attempts: %w", err)
}
