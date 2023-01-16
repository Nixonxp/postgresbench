package main

import (
	"database/sql"
	"flag"
	"fmt"
	lorem "github.com/drhodes/golorem"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/pressly/goose/v3"
	"log"
	_ "postgres_performance_test/migration"
	"postgres_performance_test/pkg/keyboard"
	"time"
)

var amount int
var commandCounter = 0

func main() {
	var err error
	amount, err = keyboard.GetIntegerInput()
	if err != nil {
		panic(err)
	}

	start := time.Now()
	log.Print("========== START ============")

	command := flag.String("c", "status", "command")
	dir := flag.String("dir", "./migration", "migration dir")
	flag.Parse()

	dsn := "postgres://test:test@localhost:5432/test?sslmode=disable"
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("-dbstring=%q: %v\n", dsn, err)
	}

	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			panic(err)
		}
		log.Print("db connection closed")
	}(db)

	if err := goose.SetDialect("postgres"); err != nil {
		panic(err)
	}

	if err := goose.Run(*command, db, *dir); err != nil {
		log.Fatalf("goose run: %v", err)
	}

	defer resetMigrations(db, dir)

	// add user tables
	nextMigrate(db, dir)
	// add articles table
	nextMigrate(db, dir)
	// add comments
	nextMigrate(db, dir)

	// add users
	insertUsers(db)
	// add articles
	insertArticles(db)
	// add comments
	insertComments(db)

	// select users
	selectFromIdUsers(db)

	// select with joins
	selectWithJoins(db)

	// select with filter
	selectWithFilters(db)

	// select with joins and filters
	selectWithJoinsAndFilters(db)

	// add nullable column
	addNullableColumn(db)

	// add column with default value
	addNullableWithDefault(db)

	// drop column test
	dropColumn(db)

	// multiline insert
	multilineInsertArticles(db)

	// bulk insert
	bulkCopy(db)

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Overall time %s", elapsed)
	log.Print("==============================")
}

func insertUsers(db *sql.DB) {
	start := time.Now()
	log.Print("========== INSERT ============")
	log.Printf("Insert %d users in progress...", amount)
	n := 1
	for n < amount {
		sqlStatement := `INSERT INTO users (id, name, description) VALUES ($1, $2, $3)`
		name := fmt.Sprint("name_", n)
		descr := fmt.Sprint("descr_", n)
		_, err := db.Exec(sqlStatement, n, name, descr)
		if err != nil {
			panic(err)
		}
		n++
	}
	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Inserted %d rows in %s", amount, elapsed)
	log.Print("==============================")
}

func insertArticles(db *sql.DB) {
	start := time.Now()
	log.Print("========== INSERT ARTICLES ============")
	log.Printf("Insert %d users in progress...", amount)
	n := 1
	for n < amount {
		sqlStatement := `INSERT INTO articles (id, author_id, title, text) VALUES ($1, $2, $3, $4)`
		title := fmt.Sprint("title_", n)
		_, err := db.Exec(sqlStatement, n, n, title, lorem.Paragraph(50, 70))
		if err != nil {
			panic(err)
		}
		n++
	}
	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Inserted %d rows in %s", amount, elapsed)
	log.Print("==============================")
}

func insertComments(db *sql.DB) {
	start := time.Now()
	log.Print("========== INSERT COMMENTS ============")
	log.Printf("Insert %d users in progress...", amount)
	n := 1
	for n < amount {
		sqlStatement := `INSERT INTO comments (id, author_id, article_id, title, text) VALUES ($1, $2, $3, $4, $5)`
		title := fmt.Sprint("title_", n)
		_, err := db.Exec(sqlStatement, n, n, n, title, lorem.Paragraph(50, 70))
		if err != nil {
			panic(err)
		}
		n++
	}
	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Inserted %d rows in %s", amount, elapsed)
	log.Print("==============================")
}

func selectFromIdUsers(db *sql.DB) {
	start := time.Now()
	log.Print("======= SELECT FROM ID =======")
	log.Printf("Select %d users in progress...", amount)
	n := 1
	for n < amount {
		sqlStatement := `SELECT * FROM users WHERE id = $1`
		_, err := db.Exec(sqlStatement, n)
		if err != nil {
			panic(err)
		}
		n++
	}
	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Selected %d rows in %s", amount, elapsed)
	log.Print("==============================")
}

func selectWithJoins(db *sql.DB) {
	start := time.Now()
	log.Print("======= SELECT ALL WITH JOIN =======")
	log.Printf("Select rows with join in progress...")

	sqlStatement := `SELECT * 
		 FROM users 
         JOIN articles ON articles.author_id = users.id
		 JOIN comments ON comments.author_id = users.id
         `
	rows, err := db.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	countRows, err := rows.RowsAffected()
	if err != nil {
		panic(err)
	}

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Selected all with join %d rows in %s", countRows, elapsed)
	log.Print("==============================")
}

func selectWithFilters(db *sql.DB) {
	start := time.Now()
	log.Print("======= SELECT WITH FILTER =======")
	log.Printf("Select rows with filter in progress...")

	sqlStatement := `SELECT * 
		 FROM users 
         WHERE name like '%name%' AND MOD(id, 2) = 0
         `
	rows, err := db.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	countRows, err := rows.RowsAffected()
	if err != nil {
		panic(err)
	}

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Selected with filter %d rows in %s", countRows, elapsed)
	log.Print("==============================")
}

func selectWithJoinsAndFilters(db *sql.DB) {
	start := time.Now()
	log.Print("======= SELECT ALL WITH JOIN AND FILTERS =======")
	log.Printf("Select rows with join and filters in progress...")

	sqlStatement := `SELECT * 
		 FROM users 
         JOIN articles ON articles.author_id = users.id
		 JOIN comments ON comments.author_id = users.id
		 WHERE users.name like '%name%' AND MOD(users.id, 2) = 0
			AND comments.title like '%tit%' AND MOD(comments.author_id, 2) = 0
         `
	rows, err := db.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	countRows, err := rows.RowsAffected()
	if err != nil {
		panic(err)
	}

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Selected all with join and filters %d rows in %s", countRows, elapsed)
	log.Print("==============================")
}

func addNullableColumn(db *sql.DB) {
	start := time.Now()
	log.Print("======= ADD NULLABLE COLUMN =======")
	log.Printf("Insert nullable column in progress...")

	sqlStatement := `ALTER TABLE users ADD COLUMN nullable_column TEXT`
	_, err := db.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Inserted nullable column in %s", elapsed)
	log.Print("==============================")
}

func addNullableWithDefault(db *sql.DB) {
	start := time.Now()
	log.Print("======= ADD COLUMN WITH DEFAULT =======")
	log.Printf("Insert new column with default value in progress...")

	sqlStatement := `ALTER TABLE users ADD COLUMN default_column TEXT NOT NULL DEFAULT 'default text in new column'`
	_, err := db.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Inserted new column with default value in %s", elapsed)
	log.Print("==============================")
}

func multilineInsertArticles(db *sql.DB) {
	start := time.Now()
	log.Print("========== MULTILINE INSERT ARTICLES ============")
	log.Printf("Multiline insert %d articles in progress...", amount)

	sqlStatement := "INSERT INTO articles (id, author_id, title, text) VALUES "

	for n := 1; n < amount; n++ {
		title := fmt.Sprint("title_", n)
		sqlStatement += fmt.Sprintf(" (%d, %d, '%s', '%s') ", n+amount*1, n, title, lorem.Paragraph(10, 15))
		if n+1 != amount {
			sqlStatement += ","
		}
	}

	_, err := db.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Multiline inserted %d rows in %s", amount, elapsed)
	log.Print("==============================")
}

func bulkCopy(db *sql.DB) {
	start := time.Now()
	log.Print("========== BULK INSERT ARTICLES ============")
	log.Printf("Bulk insert %d articles in progress...", amount)

	tx, err := db.Begin()

	if err != nil {
		panic(err)
	}

	stmt, err := tx.Prepare(pq.CopyInSchema("public", "articles", "id", "author_id", "title", "text"))
	if err != nil {
		panic(err)
	}

	for n := 1; n < amount; n++ {
		title := fmt.Sprint("title_", n)
		_, err := stmt.Exec(n+amount*2, n, title, lorem.Paragraph(10, 15))
		if err != nil {
			return
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		panic(err)
	}
	err = stmt.Close()
	if err != nil {
		panic(err)
	}
	err = tx.Commit()
	if err != nil {
		panic(err)
	}

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Bulk inserted %d rows in %s", amount, elapsed)
	log.Print("==============================")
}

func dropColumn(db *sql.DB) {
	start := time.Now()
	log.Print("======= DROP COLUMN =======")
	log.Printf("Drop column in progress...")

	sqlStatement := `ALTER TABLE users DROP COLUMN default_column`
	_, err := db.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Dropped column in %s", elapsed)
	log.Print("==============================")
}

func nextMigrate(db *sql.DB, dir *string) {
	commandCounter++
	commandName := fmt.Sprint("c_", commandCounter)
	command := flag.String(commandName, "up-by-one", "command")
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
