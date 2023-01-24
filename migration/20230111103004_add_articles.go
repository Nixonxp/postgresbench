package migration

import (
	"database/sql"
	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upAddArticles, downAddArticles)
}

func upAddArticles(tx *sql.Tx) error {
	query := `create table articles (
	id          serial       not null primary key,
	author_id   bigint references public.users (id),
	title          TEXT NOT NULL,
    text   TEXT NOT NULL);

	CREATE INDEX article_id_index
		  ON articles (id ASC);

	CREATE INDEX author_id_index
		  ON articles (author_id ASC);`
	_, err := tx.Exec(query)
	if err != nil {
		return err
	}
	return nil
}

func downAddArticles(tx *sql.Tx) error {
	query := "DROP TABLE articles"
	_, err := tx.Exec(query)
	if err != nil {
		return err
	}
	return nil
}
