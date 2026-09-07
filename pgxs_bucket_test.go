package pgxs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBucketID_String(t *testing.T) {
	tests := []struct {
		name string
		id   BucketID
		want string
	}{
		{"zero", 0, "0"},
		{"positive", 42, "42"},
		{"large", BucketID(1<<63 - 1), "9223372036854775807"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.id.String()
			require.Equal(t, tt.want, got)
		})
	}
}

func TestBucketID_Int(t *testing.T) {
	tests := []struct {
		name string
		id   BucketID
		want int
	}{
		{"zero", 0, 0},
		{"positive", 42, 42},
		{"large", BucketID(1<<63 - 1), int(1<<63 - 1)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.id.Int()
			require.Equal(t, tt.want, got)
		})
	}
}

func TestBucketID_Uint(t *testing.T) {
	tests := []struct {
		name string
		id   BucketID
		want uint
	}{
		{"zero", 0, 0},
		{"positive", 42, 42},
		{"large", BucketID(1<<63 - 1), uint(1<<63 - 1)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.id.Uint()
			require.Equal(t, tt.want, got)
		})
	}
}

func TestBucketID_Validate(t *testing.T) {
	tests := []struct {
		name    string
		id      BucketID
		max     MaxBuckets
		wantErr bool
	}{
		{"valid zero", 0, 10, false},
		{"valid middle", 5, 10, false},
		{"valid max-1", 9, 10, false},
		{"invalid equal to max", 10, 10, true},
		{"invalid greater than max", 15, 10, true},
		{"invalid huge", 100, 10, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.id.Validate(tt.max)
			if tt.wantErr {
				require.Error(t, err)
				require.Equal(t, "bucket ID out of range", err.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestMaxBuckets_Comparison(t *testing.T) {
	t.Run("Greater", func(t *testing.T) {
		require.True(t, MaxBuckets(10).Greater(5))
		require.False(t, MaxBuckets(5).Greater(10))
		require.False(t, MaxBuckets(5).Greater(5))
	})

	t.Run("GreaterOrEqual", func(t *testing.T) {
		require.True(t, MaxBuckets(10).GreaterOrEqual(5))
		require.True(t, MaxBuckets(5).GreaterOrEqual(5))
		require.False(t, MaxBuckets(5).GreaterOrEqual(10))
	})

	t.Run("Less", func(t *testing.T) {
		require.True(t, MaxBuckets(5).Less(10))
		require.False(t, MaxBuckets(10).Less(5))
		require.False(t, MaxBuckets(5).Less(5))
	})

	t.Run("LessOrEqual", func(t *testing.T) {
		require.True(t, MaxBuckets(5).LessOrEqual(10))
		require.True(t, MaxBuckets(5).LessOrEqual(5))
		require.False(t, MaxBuckets(10).LessOrEqual(5))
	})
}

func TestMaxBuckets_Uint(t *testing.T) {
	tests := []struct {
		name string
		max  MaxBuckets
		want uint
	}{
		{"zero", 0, 0},
		{"positive", 256, 256},
		{"large", MaxBuckets(1<<63 - 1), uint(1<<63 - 1)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.max.Uint()
			require.Equal(t, tt.want, got)
		})
	}
}

func TestFromInt(t *testing.T) {
	tests := []struct {
		name    string
		id      int
		max     MaxBuckets
		want    BucketID
		wantErr bool
	}{
		{"valid zero", 0, 10, 0, false},
		{"valid middle", 5, 10, 5, false},
		{"valid max-1", 9, 10, 9, false},
		{"invalid negative", -1, 10, 0, true},
		{"invalid equal to max", 10, 10, 0, true},
		{"invalid greater than max", 15, 10, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := FromInt(tt.id, tt.max)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestFromInt_ErrorMessages(t *testing.T) {
	t.Run("negative ID", func(t *testing.T) {
		_, err := FromInt(-1, 10)
		require.Error(t, err)
		require.Equal(t, "bucket ID must be non-negative", err.Error())
	})

	t.Run("out of range", func(t *testing.T) {
		_, err := FromInt(15, 10)
		require.Error(t, err)
		require.Equal(t, "bucket ID out of range", err.Error())
	})
}

func TestBucketID_TypeSafety(t *testing.T) {
	var b BucketID = 5
	var m MaxBuckets = 10

	require.Equal(t, uint(5), b.Uint())
	require.Equal(t, uint(10), m.Uint())

	err := b.Validate(m)
	require.NoError(t, err)

	err = BucketID(15).Validate(m)
	require.Error(t, err)
}
