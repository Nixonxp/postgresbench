package migration

import (
	"database/sql"
	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upAddComments, downAddComments)
}

func upAddComments(tx *sql.Tx) error {
	query := `create table comments (
	id          bigint       not null primary key,
	author_id   bigint references public.users (id),
	article_id   bigint references public.articles (id),
	title          TEXT NOT NULL,
    text   TEXT NOT NULL);`
	_, err := tx.Exec(query)
	if err != nil {
		return err
	}
	return nil
}

func downAddComments(tx *sql.Tx) error {
	query := "DROP TABLE comments"
	_, err := tx.Exec(query)
	if err != nil {
		return err
	}
	return nil
}
