package pgxs

// errorRow реализация pgx.Row, возвращающая ошибку при сканировании.
type errorRow struct{ err error }

func (r *errorRow) Scan(dest ...any) error { return r.err }
