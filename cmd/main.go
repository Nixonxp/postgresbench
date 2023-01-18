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

	dbType, err := keyboard.GetIntegerInput("Enter DB type: 1 - postgres, 2 - mongodb ")
	if err != nil {
		panic(err)
	}

	amount, err := keyboard.GetIntegerInput("Enter table rows count ")
	if err != nil {
		panic(err)
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
		postgres.StartTest(amount, poolCount, passTestCount)
	} else if dbType == 2 {
		mongodb.StartTest(amount, poolCount, passTestCount)
	} else {
		panic("Invalid DB type selected")
	}

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Overall time %s", elapsed)
	log.Print("==============================")
}
