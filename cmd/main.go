package main

import (
	_ "github.com/lib/pq"
	"log"
	"postgres_performance_test/internal/mongodb"
	"postgres_performance_test/internal/postgres"
	_ "postgres_performance_test/migration"
	"postgres_performance_test/pkg/keyboard"
	"time"
)

func main() {
	var err error
	amount := 10000

	dbType, err := keyboard.GetIntegerInput("Enter DB type: 1 - postgres, 2 - mongodb ")
	if err != nil {
		panic(err)
	}

	useTestSchema, err := keyboard.GetIntegerInput("Use test schema [100k users, 1 m articles, 10 m comments], 0 - no, 1 - yes , default - 0 :")
	if err != nil {
		useTestSchema = 0
	}

	if useTestSchema == 0 {
		amount, err = keyboard.GetIntegerInput("Enter table rows count ")
		if err != nil {
			panic(err)
		}
	}

	poolCount, err := keyboard.GetIntegerInput("Enter connection pool size ")
	if err != nil {
		panic(err)
	}

	if poolCount <= 0 {
		poolCount = 100
	}

	passTestCount, err := keyboard.GetIntegerInput("Pass test count, default - 0 ")
	if err != nil {
		passTestCount = 0
	}

	start := time.Now()
	log.Print("========== START ============")

	if dbType == 1 {
		runMigrations, err := keyboard.GetIntegerInput("Run migrations 0 - yes, 1 - no, default - 0 ")
		if err != nil {
			runMigrations = 0
		}
		postgres.StartTest(amount, poolCount, passTestCount, runMigrations, useTestSchema)
	} else if dbType == 2 {
		mongodb.StartTest(amount, poolCount, passTestCount, useTestSchema)
	} else {
		panic("Invalid DB type selected")
	}

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Overall time %s", elapsed)
	log.Print("==============================")
}
