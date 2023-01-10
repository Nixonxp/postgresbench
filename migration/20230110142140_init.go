package migration

import (
	"database/sql"
	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upInit, downInit)
}

func upInit(tx *sql.Tx) error {
	query := `create table users (
	id          bigint       not null primary key,
	name          TEXT NOT NULL,
    description   TEXT NOT NULL);`
	_, err := tx.Exec(query)
	if err != nil {
		return err
	}
	return nil
}

func downInit(tx *sql.Tx) error {
	_, err := tx.Exec("DROP TABLE users")
	if err != nil {
		return err
	}

	return nil
}
