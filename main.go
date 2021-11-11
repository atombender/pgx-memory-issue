package main

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/jackc/pgx/v4/pgxpool"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	go func() {
		for {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			log.Printf("heap: %10d MB", m.HeapAlloc/(1024*1024))
			time.Sleep(2 * time.Second)
		}
	}()

	run()

	log.Printf("trying to GC more")
	for i := 1; i <= 5; i++ {
		runtime.GC()
		time.Sleep(1 * time.Second)
	}

	select {}
}

func run() {
	ctx := context.Background()

	connConfig, err := pgxpool.ParseConfig("")
	if err != nil {
		log.Fatal(err)
	}
	connConfig.MaxConns = 20
	connConfig.MaxConnIdleTime = 1 * time.Second
	connConfig.HealthCheckPeriod = 1 * time.Second
	connConfig.ConnConfig.BuildStatementCache = nil

	pool, err := pgxpool.ConnectConfig(ctx, connConfig)
	if err != nil {
		log.Fatal(err)
	}

	pool.AcquireFunc(ctx, func(c *pgxpool.Conn) error {
		_, err = pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS pgxtest (id serial PRIMARY KEY, val bytea NOT NULL)`)
		if err != nil {
			log.Fatal(err)
		}
		return nil
	})

	data := make([]byte, 10_000)

	var wg sync.WaitGroup
	for i := 1; i <= 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pool.AcquireFunc(ctx, func(conn *pgxpool.Conn) error {
				count := 100_000_000 / len(data)

				var sql strings.Builder
				sql.WriteString("INSERT INTO pgxtest (val) VALUES")

				params := make([]interface{}, 0, count)
				for i := 1; i <= count; i++ {
					if i > 1 {
						sql.WriteString(", ")
					}
					sql.WriteString(fmt.Sprintf("($%d)", i))
					params = append(params, data)
				}

				cmdTag, err := conn.Exec(ctx, sql.String(), params...)
				if err != nil {
					log.Fatal(err)
				}
				log.Printf("inserted %d rows (%d bytes)", cmdTag.RowsAffected(), count*len(data))
				return nil
			})
		}()
	}
	wg.Wait()

	log.Printf("waiting for conns to close")
	for {
		time.Sleep(1 * time.Second)
		stats := pool.Stat()
		log.Printf("  pool idle: %d active: %d", stats.IdleConns(), stats.AcquiredConns())
		if stats.IdleConns() == 0 {
			break
		}
	}

	// Add to prevent leak:
	//pool.Close()
}
