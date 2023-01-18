package postgres

import (
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"github.com/lib/pq"
	"github.com/pressly/goose/v3"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

var amount int
var poolCount int
var countInWorker int
var commandCounter = 0
var isUseTestSchema = false
var loremText = "Lorem Ipsum - это текст-\"рыба\", часто используемый в печати и вэб-дизайне. Lorem Ipsum является стандартной \"рыбой\" для текстов на латинице с начала XVI века. В то время некий безымянный печатник создал большую коллекцию размеров и форм шрифтов, используя Lorem Ipsum для распечатки образцов. Lorem Ipsum не только успешно пережил без заметных изменений пять веков, но и перешагнул в электронный дизайн. Его популяризации в новое время послужили публикация листов Letraset с образцами Lorem Ipsum в 60-х годах и, в более недавнее время, программы электронной вёрстки типа Aldus PageMaker, в шаблонах которых используется Lorem Ipsum."

var wg sync.WaitGroup

func StartTest(amountRows, poolCountSize, passTestCount, runMigrations, useTestSchema int) {
	amount = amountRows
	poolCount = poolCountSize

	if useTestSchema != 0 {
		isUseTestSchema = true
	}

	countInWorker = int(amount / poolCount)

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

	if runMigrations == 0 {
		// add user tables
		nextMigrate(db, dir)
		// add articles table
		nextMigrate(db, dir)
		// add comments
		nextMigrate(db, dir)
		// add simple articles and comments table
		nextMigrate(db, dir)
	}

	if passTestCount < 1 {
		// add users
		insertUsers(db)
	}

	if passTestCount < 2 {
		// add articles
		insertArticles(db)
	}

	if passTestCount < 3 {
		// add articles without references
		insertArticlesWithoutReferences(db)
	}

	if passTestCount < 4 {
		// add comments
		insertComments(db)
	}

	if passTestCount < 5 {
		// add comments without references
		insertCommentsWithoutReferences(db)
	}

	if passTestCount < 6 {
		// select users
		selectFromIdUsers(db)
	}

	if passTestCount < 7 {
		// select with joins
		selectWithJoins(db)
	}

	if passTestCount < 8 {
		// select with filter
		selectWithFilters(db)
	}

	if passTestCount < 9 {
		// select with joins and filters
		selectWithJoinsAndFilters(db)
	}

	if passTestCount < 10 {
		// add nullable column
		addNullableColumn(db)
	}

	if passTestCount < 11 {
		// add column with default value
		addNullableWithDefault(db)
	}

	if passTestCount < 12 {
		// drop column test
		dropColumn(db)
	}

	if passTestCount < 13 {
		// multiline insert
		multilineInsertArticles(db)
	}

	if passTestCount < 14 {
		// bulk insert
		bulkCopy(db)
	}
}

func insertUsers(db *sql.DB) {
	if isUseTestSchema == true {
		amount = 100000
	}
	countInWorker = int(amount / poolCount)

	start := time.Now()
	log.Print("========== INSERT ============")
	log.Printf("Insert %d users in progress...", amount)
	log.Printf("Use connection pool size = %d", poolCount)

	for i := 0; i < poolCount; i++ {
		wg.Add(1)
		go func(db *sql.DB, countInWorker, i int) {
			defer wg.Done()
			maxDiapason := (i + 1) * countInWorker
			for currentPosition := i * countInWorker; currentPosition < maxDiapason; currentPosition++ {
				sqlStatement := `INSERT INTO users (id, name, description) VALUES ($1, $2, $3)`
				name := fmt.Sprint("name_", currentPosition)
				descr := fmt.Sprint("descr_", currentPosition)
				_, err := db.Exec(sqlStatement, currentPosition, name, descr)
				if err != nil {
					panic(err)
				}
			}
		}(db, countInWorker, i)
	}

	wg.Wait()

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Inserted %d rows in %s", amount, elapsed)
	log.Print("==============================")
}

func getRandomInt(min, max int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(max-min) + min
}

func insertArticles(db *sql.DB) {
	if isUseTestSchema == true {
		amount = 1000000
	}
	countInWorker = int(amount / poolCount)
	var authorId int
	start := time.Now()
	log.Print("========== INSERT ARTICLES ============")
	log.Printf("Insert %d articles in progress...", amount)
	log.Printf("Use connection pool size = %d", poolCount)

	for i := 0; i < poolCount; i++ {
		wg.Add(1)
		go func(db *sql.DB, countInWorker, i int) {
			defer wg.Done()
			maxDiapason := (i + 1) * countInWorker
			for currentPosition := i * countInWorker; currentPosition < maxDiapason; currentPosition++ {
				sqlStatement := `INSERT INTO articles (id, author_id, title, text) VALUES ($1, $2, $3, $4)`
				title := fmt.Sprint("title_", currentPosition)

				if isUseTestSchema == true {
					authorId = getRandomInt(0, 100000)
				} else {
					authorId = currentPosition
				}

				_, err := db.Exec(sqlStatement, currentPosition, authorId, title, loremText)
				if err != nil {
					panic(err)
				}
			}
		}(db, countInWorker, i)
	}

	wg.Wait()

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Inserted %d rows in %s", amount, elapsed)
	log.Print("==============================")
}

func insertArticlesWithoutReferences(db *sql.DB) {
	start := time.Now()
	log.Print("========== INSERT ARTICLES WITHOUT REFERENCES =================")
	log.Printf("Insert %d users in progress...", amount)
	log.Printf("Use connection pool size = %d", poolCount)

	for i := 0; i < poolCount; i++ {
		wg.Add(1)
		go func(db *sql.DB, countInWorker, i int) {
			defer wg.Done()
			maxDiapason := (i + 1) * countInWorker
			for currentPosition := i * countInWorker; currentPosition < maxDiapason; currentPosition++ {
				sqlStatement := `INSERT INTO articles_simple (id, author_id, title, text) VALUES ($1, $2, $3, $4)`
				title := fmt.Sprint("title_", currentPosition)
				_, err := db.Exec(sqlStatement, currentPosition, currentPosition, title, loremText)
				if err != nil {
					panic(err)
				}
			}
		}(db, countInWorker, i)
	}

	wg.Wait()

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Inserted %d rows in %s", amount, elapsed)
	log.Print("==============================")
}

func insertComments(db *sql.DB) {
	if isUseTestSchema == true {
		amount = 10000000
	}
	countInWorker = int(amount / poolCount)
	var authorId int
	var articleId int

	start := time.Now()
	log.Print("========== INSERT COMMENTS ============")
	log.Printf("Insert %d users in progress...", amount)
	log.Printf("Use connection pool size = %d", poolCount)

	for i := 0; i < poolCount; i++ {
		wg.Add(1)
		go func(db *sql.DB, countInWorker, i int) {
			defer wg.Done()
			maxDiapason := (i + 1) * countInWorker
			for currentPosition := i * countInWorker; currentPosition < maxDiapason; currentPosition++ {
				sqlStatement := `INSERT INTO comments (id, author_id, article_id, title, text) VALUES ($1, $2, $3, $4, $5)`
				title := fmt.Sprint("title_", currentPosition)

				if isUseTestSchema == true {
					authorId = getRandomInt(0, 100000)
					articleId = getRandomInt(0, 1000000)
				} else {
					authorId = currentPosition
					articleId = currentPosition
				}

				_, err := db.Exec(sqlStatement, currentPosition, authorId, articleId, title, loremText)
				if err != nil {
					panic(err)
				}
			}
		}(db, countInWorker, i)
	}

	wg.Wait()

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Inserted %d rows in %s", amount, elapsed)
	log.Print("==============================")
}

func insertCommentsWithoutReferences(db *sql.DB) {
	start := time.Now()
	log.Print("========== INSERT COMMENTS WITHOUT REFERENCES =================")
	log.Printf("Insert %d users in progress...", amount)
	log.Printf("Use connection pool size = %d", poolCount)

	for i := 0; i < poolCount; i++ {
		wg.Add(1)
		go func(db *sql.DB, countInWorker, i int) {
			defer wg.Done()
			maxDiapason := (i + 1) * countInWorker
			for currentPosition := i * countInWorker; currentPosition < maxDiapason; currentPosition++ {
				sqlStatement := `INSERT INTO comments_simple (id, author_id, article_id, title, text) VALUES ($1, $2, $3, $4, $5)`
				title := fmt.Sprint("title_", currentPosition)
				_, err := db.Exec(sqlStatement, currentPosition, currentPosition, currentPosition, title, loremText)
				if err != nil {
					panic(err)
				}
			}
		}(db, countInWorker, i)
	}

	wg.Wait()

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Inserted %d rows in %s", amount, elapsed)
	log.Print("==============================")
}

func selectFromIdUsers(db *sql.DB) {
	if isUseTestSchema == true {
		amount = 100000
	}
	countInWorker = int(amount / poolCount)
	start := time.Now()
	log.Print("======= SELECT FROM ID =======")
	log.Printf("Select %d users in progress...", amount)

	for i := 0; i < poolCount; i++ {
		wg.Add(1)
		go func(db *sql.DB, countInWorker, i int) {
			defer wg.Done()
			maxDiapason := (i + 1) * countInWorker
			for currentPosition := i * countInWorker; currentPosition < maxDiapason; currentPosition++ {
				sqlStatement := `SELECT * FROM users WHERE id = $1`
				_, err := db.Exec(sqlStatement, currentPosition)
				if err != nil {
					panic(err)
				}
			}
		}(db, countInWorker, i)
	}

	wg.Wait()

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
	if isUseTestSchema == true {
		amount = 1000000
	}
	start := time.Now()
	log.Print("========== MULTILINE INSERT ARTICLES ============")
	log.Printf("Multiline insert %d articles in progress...", amount)

	var buffer bytes.Buffer
	buffer.WriteString("INSERT INTO articles (id, author_id, title, text) VALUES ")

	for n := 1; n < amount; n++ {
		buffer.WriteString(fmt.Sprintf(" (%d, %d, '%s', '%s') ", n+amount*1, n, "title_"+strconv.Itoa(n), "text article"))
		if n+1 != amount {
			buffer.WriteString(",")
		}
	}

	_, err := db.Exec(buffer.String())
	if err != nil {
		panic(err)
	}

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Multiline inserted %d rows in %s", amount, elapsed)
	log.Print("==============================")
}

func bulkCopy(db *sql.DB) {
	if isUseTestSchema == true {
		amount = 1000000
	}
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
		_, err := stmt.Exec(n+amount*2, n, title, loremText)
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
