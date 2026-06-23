package api_storage

func clampPage(p int) int {
	if p <= 0 {
		return 1
	}
	return p
}

func clampBlockListPageSize(ps int) int {
	if ps <= 0 || ps > 100 {
		return 10
	}
	return ps
}

func clampTxListPageSize(ps int) int {
	if ps <= 0 || ps > 1000 {
		return 100
	}
	return ps
}

func clampErc20PageSize(ps int) int {
	if ps <= 0 || ps > 500 {
		return 50
	}
	return ps
}

func paginationOffset(page, pageSize int) int {
	return (page - 1) * pageSize
}
