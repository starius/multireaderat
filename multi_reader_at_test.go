package multireaderat

import (
	"bytes"
	"io"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMultiReaderAt(t *testing.T) {
	text := []byte("The quick brown fox jumps over the lazy dog")

	sizes := []int64{3, 5, 1, 10, 7, 11, 6}

	partReaders := make([]io.ReaderAt, len(sizes))
	start := int64(0)
	for i, size := range sizes {
		partReaders[i] = bytes.NewReader(text[start : start+size])
		start += size
	}
	wholeSize := start
	require.Equal(t, len(text), int(wholeSize))

	mr, err := New(partReaders, sizes)
	require.NoError(t, err)

	t.Run("read all", func(t *testing.T) {
		r := io.NewSectionReader(mr, 0, wholeSize)
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, text, got)
	})

	t.Run("read slice", func(t *testing.T) {
		for begin := int64(0); begin <= wholeSize; begin++ {
			for end := begin; end <= wholeSize; end++ {
				size := end - begin
				buf := make([]byte, size)
				n, err := mr.ReadAt(buf, begin)
				require.NoError(t, err)
				require.Equal(t, int(size), n)
				require.Equal(t, text[begin:end], buf)
			}
		}
	})

	t.Run("read slice with too large size", func(t *testing.T) {
		end := wholeSize + 10
		for begin := int64(0); begin <= wholeSize; begin++ {
			size := end - begin
			buf := make([]byte, size)
			n, err := mr.ReadAt(buf, begin)
			require.ErrorIs(t, err, io.EOF)
			require.Equal(t, int(wholeSize-begin), n)
			require.Equal(t, text[begin:], buf[:n])
		}
	})

	t.Run("read slice with too large offset", func(t *testing.T) {
		begin := wholeSize + 5
		end := wholeSize + 10
		size := end - begin
		buf := make([]byte, size)
		n, err := mr.ReadAt(buf, begin)
		require.ErrorIs(t, err, io.EOF)
		require.Equal(t, 0, n)
	})
}

func TestMultiReaderAtLarge(t *testing.T) {
	rng := rand.New(rand.NewSource(12345))

	wholeSize := int64(1024 * 1024)
	text := make([]byte, wholeSize)
	n, err := rng.Read(text)
	require.NoError(t, err)
	require.Equal(t, int(wholeSize), n)

	numParts := 10
	sizeFrom := int(wholeSize) / numParts / 2
	sizeTo := sizeFrom * 3
	sizes := make([]int64, numParts)
	sum := int64(0)
	for i := 1; i < numParts; i++ {
		sizes[i] = int64(sizeFrom + rng.Intn(sizeTo-sizeFrom))
		sum += sizes[i]
	}
	sizes[0] = wholeSize - sum
	if sizes[0] <= 0 {
		t.Fatal("change random seed above")
	}

	partReaders := make([]io.ReaderAt, numParts)
	start := int64(0)
	for i, size := range sizes {
		partReaders[i] = bytes.NewReader(text[start : start+size])
		start += size
	}
	require.Equal(t, wholeSize, start)

	mr, err := New(partReaders, sizes)
	require.NoError(t, err)

	t.Run("read all", func(t *testing.T) {
		r := io.NewSectionReader(mr, 0, wholeSize)
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, text, got)
	})

	t.Run("read slice", func(t *testing.T) {
		for begin := int64(0); begin <= wholeSize; begin += int64(1 + rng.Intn(50000)) {
			for end := begin; end <= wholeSize; end += int64(1 + rng.Intn(50000)) {
				size := end - begin
				buf := make([]byte, size)
				n, err := mr.ReadAt(buf, begin)
				require.NoError(t, err)
				require.Equal(t, int(size), n)
				require.Equal(t, text[begin:end], buf)
			}
		}
	})

	t.Run("read slice with too large size", func(t *testing.T) {
		begin := wholeSize / 2
		end := wholeSize + 10
		size := end - begin
		buf := make([]byte, size)
		n, err := mr.ReadAt(buf, begin)
		require.ErrorIs(t, err, io.EOF)
		require.Equal(t, int(wholeSize-begin), n)
		require.Equal(t, text[begin:], buf[:n])
	})

	t.Run("read slice with too large offset", func(t *testing.T) {
		begin := wholeSize + 5
		end := wholeSize + 10
		size := end - begin
		buf := make([]byte, size)
		n, err := mr.ReadAt(buf, begin)
		require.ErrorIs(t, err, io.EOF)
		require.Equal(t, 0, n)
	})
}
