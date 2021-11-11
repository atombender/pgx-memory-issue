package main

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"

	"net/http"
	_ "net/http/pprof"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	ctx := context.Background()

	connConfig, err := pgxpool.ParseConfig("")
	if err != nil {
		log.Fatal(err)
	}
	connConfig.MaxConns = 20

	// Does not help?
	// connConfig.AfterRelease = func(conn *pgx.Conn) bool {
	// 	return false
	// }

	pool, err := pgxpool.ConnectConfig(ctx, connConfig)
	if err != nil {
		log.Fatal(err)
	}

	_, err = pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS pgxtest (id serial PRIMARY KEY, val bytea NOT NULL)`)
	if err != nil {
		log.Fatal(err)
	}

	stopCh := make(chan struct{})

	data := make([]byte, 10_000)

	var wg sync.WaitGroup
	for i := 1; i <= 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			conn, err := pool.Acquire(ctx)
			if err != nil {
				log.Fatal(err)
			}
			defer conn.Release()

			if err := conn.BeginFunc(ctx, func(t pgx.Tx) error {
				log.Printf("inserting...")

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
				log.Printf("inserted %d", cmdTag.RowsAffected())

				sql.Reset()
				params = nil
				return nil
			}); err != nil {
				log.Fatal(err)
			}
		}()
	}
	wg.Wait()

	close(stopCh)

	log.Printf("idle, trying to GC")
	for i := 1; i <= 5; i++ {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		log.Printf("heap=%10d", m.HeapAlloc)
	}

	pool.Close()
	log.Printf("closed pool")
	for i := 1; i <= 5; i++ {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		log.Printf("heap=%10d", m.HeapAlloc)
	}
	select {}
}
