package pgxs

import (
	"errors"
	"strconv"
)

const BucketPattern = "{schema}"

// BucketID представляет номер бакета (схемы) - bucket id.
type BucketID uint

// MaxBuckets представляет максимальное количество бакетов в кластере.
type MaxBuckets uint

// String возвращает строковое представление BucketID.
func (b BucketID) String() string {
	return strconv.Itoa(int(b))
}

// Greater проверяет, превышает ли MaxBuckets значение BucketID.
// Возвращает true, если max > id.
func (m MaxBuckets) Greater(id BucketID) bool {
	return uint(m) > uint(id)
}

// GreaterOrEqual проверяет, больше или равно MaxBuckets значению BucketID.
func (m MaxBuckets) GreaterOrEqual(id BucketID) bool {
	return uint(m) >= uint(id)
}

// Less проверяет, меньше ли MaxBuckets значения BucketID.
func (m MaxBuckets) Less(id BucketID) bool {
	return uint(m) < uint(id)
}

// LessOrEqual проверяет, меньше или равно MaxBuckets значению BucketID.
func (m MaxBuckets) LessOrEqual(id BucketID) bool {
	return uint(m) <= uint(id)
}

// Validate проверяет, что BucketID находится в допустимом диапазоне [0, max).
func (b BucketID) Validate(max MaxBuckets) error {
	if uint(b) >= uint(max) {
		return errors.New("bucket BucketID out of range")
	}
	return nil
}

// Int возвращает BucketID как int (для совместимости с существующим кодом).
func (b BucketID) Int() int {
	return int(b)
}

// Uint возвращает BucketID как uint.
func (b BucketID) Uint() uint {
	return uint(b)
}

// FromInt создаёт BucketID из int с проверкой диапазона.
func FromInt(id int, max MaxBuckets) (BucketID, error) {
	if id < 0 {
		return 0, errors.New("bucket BucketID must be non-negative")
	}
	b := BucketID(id)
	if err := b.Validate(max); err != nil {
		return 0, err
	}
	return b, nil
}

// Uint возвращает BucketID как uint.
func (b MaxBuckets) Uint() uint {
	return uint(b)
}
