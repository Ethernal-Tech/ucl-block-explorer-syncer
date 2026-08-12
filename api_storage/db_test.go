package api_storage

import (
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestSetDBDisablesDataAnchorExprWhenTablesMissing(t *testing.T) {
	conn, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sql mock: %v", err)
	}
	defer conn.Close()

	previous := db

	t.Cleanup(func() {
		db = previous

		resetDataAnchorSchemaStateForTest()
	})

	mock.ExpectQuery("SELECT to_regclass").
		WillReturnRows(sqlmock.NewRows([]string{"ready"}).AddRow(false))

	SetDB(conn)

	if got := dataAnchorSQLExpr(); got != isDataAnchorSQLExprDisabled {
		t.Fatalf("dataAnchorSQLExpr: got %q want FALSE", got)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDataAnchorSQLExprReprobesAfterTablesAppear(t *testing.T) {
	conn, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sql mock: %v", err)
	}
	defer conn.Close()

	previous := db

	t.Cleanup(func() {
		db = previous

		resetDataAnchorSchemaStateForTest()
	})

	mock.ExpectQuery("SELECT to_regclass").
		WillReturnRows(sqlmock.NewRows([]string{"ready"}).AddRow(false))
	SetDB(conn)

	if got := dataAnchorSQLExpr(); got != isDataAnchorSQLExprDisabled {
		t.Fatalf("before migration: got %q want FALSE", got)
	}

	dataAnchorMu.Lock()
	dataAnchorLastProbe = time.Now().UTC().Add(-dataAnchorSchemaProbeInterval)
	dataAnchorMu.Unlock()

	mock.ExpectQuery("SELECT to_regclass").
		WillReturnRows(sqlmock.NewRows([]string{"ready"}).AddRow(true))

	if got := dataAnchorSQLExpr(); got != isDataAnchorSQLExprEnabled {
		t.Fatalf("after migration: expression not re-enabled")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDataAnchorSQLExprDefaultsEnabledWithoutSetDB(t *testing.T) {
	t.Cleanup(resetDataAnchorSchemaStateForTest)

	resetDataAnchorSchemaStateForTest()

	if got := dataAnchorSQLExpr(); got != isDataAnchorSQLExprEnabled {
		t.Fatalf("default expr: got %q", got)
	}
}
