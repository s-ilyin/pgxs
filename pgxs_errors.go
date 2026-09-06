package pgxs

import "fmt"

// RetryError — ошибка после исчерпания попыток.
type RetryError struct {
	Err      error
	Attempts int
}

func (e *RetryError) Error() string {
	return fmt.Sprintf("retry failed after %d attempts: %v", e.Attempts, e.Err)
}

func (e *RetryError) Unwrap() error {
	return e.Err
}
