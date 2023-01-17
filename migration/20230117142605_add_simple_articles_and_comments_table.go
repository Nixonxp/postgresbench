package migration

import (
	"database/sql"
	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upAddSimpleArticlesAndCommentsTable, downAddSimpleArticlesAndCommentsTable)
}

func upAddSimpleArticlesAndCommentsTable(tx *sql.Tx) error {
	query := `create table articles_simple (
	id          serial       not null primary key,
	author_id   bigint,
	title          TEXT NOT NULL,
    text   TEXT NOT NULL);

create table comments_simple (
	id          bigint       not null primary key,
	author_id   bigint,
	article_id   bigint,
	title          TEXT NOT NULL,
    text   TEXT NOT NULL);`
	_, err := tx.Exec(query)
	if err != nil {
		return err
	}
	return nil
}

func downAddSimpleArticlesAndCommentsTable(tx *sql.Tx) error {
	query := `DROP TABLE articles_simple; 
DROP TABLE comments_simple;`
	_, err := tx.Exec(query)
	if err != nil {
		return err
	}
	return nil
}
