package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/pressly/goose/v3"
	"log"
	_ "postgres_performance_test/migration"
	"time"
)

func main() {
	command := flag.String("c", "status", "command")
	dir := flag.String("dir", "./migration", "migration dir")
	flag.Parse()

	dsn := "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("-dbstring=%q: %v\n", dsn, err)
	}

	if err := goose.SetDialect("postgres"); err != nil {
		panic(err)
	}

	if err := goose.Run(*command, db, *dir); err != nil {
		log.Fatalf("goose run: %v", err)
	}

	defer resetMigrations(db, dir)

	// add user tables
	nextMigrate(db, dir)

	// add users
	start := time.Now()
	amountUsers := 5000
	log.Printf("Insert %d users in progress...", amountUsers)
	insertUsers(db, amountUsers)
	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Inserted %d rows in %s", amountUsers, elapsed)
}

func insertUsers(db *sql.DB, amountUsers int) {
	n := 1
	for n < amountUsers {
		sqlStatement := `INSERT INTO users (id, name, description) VALUES ($1, $2, $3)`
		name := fmt.Sprint("name_", n)
		descr := fmt.Sprint("descr_", n)
		_, err := db.Exec(sqlStatement, n, name, descr)
		if err != nil {
			panic(err)
		}
		n++
	}
}

func nextMigrate(db *sql.DB, dir *string) {
	command := flag.String("c2", "up-by-one", "command")
	flag.Parse()

	if err := goose.Run(*command, db, *dir); err != nil {
		log.Fatalf("goose run: %v", err)
	}
}

func resetMigrations(db *sql.DB, dir *string) {
	log.Print("Reset all migrations...")
	command := flag.String("c3", "reset", "command")
	flag.Parse()

	if err := goose.Run(*command, db, *dir); err != nil {
		log.Fatalf("goose run reset: %v", err)
	}
}
