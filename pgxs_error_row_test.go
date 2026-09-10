package pgxs

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrorRow_Scan(t *testing.T) {
	t.Run("returns stored error", func(t *testing.T) {
		row := &errorRow{err: assert.AnError}

		var dest string
		err := row.Scan(&dest)
		require.Error(t, err)
		require.Equal(t, assert.AnError, err)
		// dest остаётся нулевым (не изменяется)
		require.Equal(t, "", dest)
	})

	t.Run("returns nil if error is nil", func(t *testing.T) {
		row := &errorRow{err: nil}

		var dest int
		err := row.Scan(&dest)
		require.NoError(t, err)
		require.Equal(t, 0, dest)
	})

	t.Run("ignores dest arguments", func(t *testing.T) {
		expectedErr := errors.New("another error")
		row := &errorRow{err: expectedErr}

		var a, b, c string
		err := row.Scan(&a, &b, &c)
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
		// Все dest остаются нулевыми
		require.Equal(t, "", a)
		require.Equal(t, "", b)
		require.Equal(t, "", c)
	})

	t.Run("with multiple dest and non-nil error", func(t *testing.T) {
		row := &errorRow{err: assert.AnError}

		var (
			x int
			y float64
			z bool
		)
		err := row.Scan(&x, &y, &z)
		require.Error(t, err)
		require.Equal(t, assert.AnError, err)
		// значения не меняются
		require.Equal(t, 0, x)
		require.Equal(t, 0.0, y)
		require.Equal(t, false, z)
	})
}
