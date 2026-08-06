package api_storage

import (
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestGetDailyCommitmentsFiltersAndPaginates(t *testing.T) {
	conn, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sql mock: %v", err)
	}
	defer conn.Close()

	previous := db
	db = conn

	t.Cleanup(func() { db = previous })

	dayFrom := int64(86400)
	dayTo := int64(172800)
	factory := "0x1000000000000000000000000000000000000001"
	institution := "0x0000000000000000000000000000000000000000000000000000000000000001"
	dataType := "0x0000000000000000000000000000000000000000000000000000000000000002"

	mock.ExpectQuery("SELECT factory_address, day_timestamp").
		WithArgs(factory, dayFrom, dayTo, institution, dataType, 10, 3).
		WillReturnRows(sqlmock.NewRows([]string{
			"factory_address",
			"day_timestamp",
			"data_type",
			"institution_id",
			"daily_contract_address",
			"commitment_count",
			"discovery_block",
		}).AddRow(
			factory,
			dayTo,
			dataType,
			institution,
			"0x2000000000000000000000000000000000000002",
			2,
			100,
		))

	resp, err := GetDailyCommitments(DailyCommitmentsRequest{
		FactoryAddress: factory,
		DayFrom:        &dayFrom,
		DayTo:          &dayTo,
		InstitutionID:  institution,
		DataType:       dataType,
		Limit:          10,
		Offset:         3,
	})
	if err != nil {
		t.Fatalf("get daily commitments: %v", err)
	}

	if len(resp.List) != 1 || resp.List[0].CommitmentCount != 2 ||
		resp.Limit != 10 || resp.Offset != 3 {
		t.Fatalf("unexpected response: %+v", resp)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestGetDailyCommitmentsValidation(t *testing.T) {
	t.Parallel()

	negative := int64(-1)
	earlier := int64(172800)
	later := int64(86400)

	tests := []struct {
		request   DailyCommitmentsRequest
		parameter string
	}{
		{DailyCommitmentsRequest{Limit: MaxDailyCommitmentsLimit + 1}, "limit"},
		{DailyCommitmentsRequest{Offset: -1}, "offset"},
		{DailyCommitmentsRequest{DayFrom: &negative}, "day_from"},
		{DailyCommitmentsRequest{DayFrom: &earlier, DayTo: &later}, "day_from"},
		{DailyCommitmentsRequest{FactoryAddress: "not-an-address"}, "factory_address"},
		{DailyCommitmentsRequest{InstitutionID: "0x01"}, "institution_id"},
		{DailyCommitmentsRequest{DataType: "0x02"}, "data_type"},
	}
	for _, test := range tests {
		_, err := GetDailyCommitments(test.request)
		if err == nil {
			t.Fatalf("expected validation error for %+v", test.request)
		}

		var validationErr *DailyCommitmentsValidationError
		if !errors.As(err, &validationErr) {
			t.Fatalf("expected typed validation error for %+v, got %T", test.request, err)
		}

		if validationErr.Parameter != test.parameter {
			t.Fatalf("validation parameter: got %q want %q",
				validationErr.Parameter, test.parameter)
		}
	}
}
