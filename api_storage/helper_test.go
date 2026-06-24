package api_storage

import (
	"testing"
)

func TestClampPage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input int
		want  int
	}{
		{-5, 1},
		{0, 1},
		{1, 1},
		{2, 2},
		{100, 100},
	}
	for _, tc := range tests {
		tc := tc

		t.Run("", func(t *testing.T) {
			t.Parallel()

			if got := clampPage(tc.input); got != tc.want {
				t.Fatalf("clampPage(%d) = %d, want %d", tc.input, got, tc.want)
			}
		})
	}
}

func TestClampBlockListPageSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input int
		want  int
	}{
		{-1, 10},
		{0, 10},
		{1, 1},
		{50, 50},
		{100, 100},
		{101, 10},
		{9999, 10},
	}
	for _, tc := range tests {
		tc := tc

		t.Run("", func(t *testing.T) {
			t.Parallel()

			if got := clampBlockListPageSize(tc.input); got != tc.want {
				t.Fatalf("clampBlockListPageSize(%d) = %d, want %d", tc.input, got, tc.want)
			}
		})
	}
}

func TestClampTxListPageSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input int
		want  int
	}{
		{-1, 100},
		{0, 100},
		{1, 1},
		{100, 100},
		{1000, 1000},
		{1001, 100},
		{5000, 100},
	}
	for _, tc := range tests {
		tc := tc

		t.Run("", func(t *testing.T) {
			t.Parallel()

			if got := clampTxListPageSize(tc.input); got != tc.want {
				t.Fatalf("clampTxListPageSize(%d) = %d, want %d", tc.input, got, tc.want)
			}
		})
	}
}

func TestClampErc20PageSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input int
		want  int
	}{
		{-1, 50},
		{0, 50},
		{1, 1},
		{50, 50},
		{500, 500},
		{501, 50},
		{9999, 50},
	}
	for _, tc := range tests {
		tc := tc

		t.Run("", func(t *testing.T) {
			t.Parallel()

			if got := clampErc20PageSize(tc.input); got != tc.want {
				t.Fatalf("clampErc20PageSize(%d) = %d, want %d", tc.input, got, tc.want)
			}
		})
	}
}

func TestPaginationOffset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		page     int
		pageSize int
		want     int
	}{
		{1, 10, 0},
		{2, 10, 10},
		{3, 10, 20},
		{5, 25, 100},
		{1, 1, 0},
		{100, 100, 9900},
	}

	for _, tc := range tests {
		tc := tc

		t.Run("", func(t *testing.T) {
			t.Parallel()

			got := paginationOffset(tc.page, tc.pageSize)
			if got != tc.want {
				t.Fatalf("paginationOffset(%d, %d) = %d, want %d", tc.page, tc.pageSize, got, tc.want)
			}
		})
	}
}
