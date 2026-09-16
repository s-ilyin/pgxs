package pgxs

import (
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type batchResultsWithError struct {
	err error
}

func (b *batchResultsWithError) Exec() (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, b.err
}

func (b *batchResultsWithError) Query() (pgx.Rows, error) {
	return nil, b.err
}

func (b *batchResultsWithError) QueryRow() pgx.Row {
	return &errorRow{err: b.err}
}

func (b *batchResultsWithError) Close() error {
	return b.err
}
