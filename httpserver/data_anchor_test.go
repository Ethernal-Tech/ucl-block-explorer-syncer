package httpserver

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/api_storage"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/explorer"
)

const testDataAnchorFactoryAddress = "0x1000000000000000000000000000000000000001"

func TestDailyCommitmentsPublicRouteDoesNotRequireCredentials(t *testing.T) {
	t.Parallel()

	s := New(explorer.NewExplorer(), Config{})
	s.getDailyCommitments = func(req api_storage.DailyCommitmentsRequest) (
		*api_storage.DailyCommitmentsResponse,
		error,
	) {
		if req.FactoryAddress != testDataAnchorFactoryAddress ||
			req.InstitutionID == "" || req.DataType == "" || req.Limit != 10 ||
			req.Offset != 3 || req.DayFrom == nil || req.DayTo == nil {
			t.Fatalf("unexpected request: %+v", req)
		}

		return &api_storage.DailyCommitmentsResponse{
			List: []api_storage.DailyCommitmentItem{{
				FactoryAddress:       testDataAnchorFactoryAddress,
				DayTimestamp:         172800,
				DataType:             req.DataType,
				InstitutionID:        req.InstitutionID,
				DailyContractAddress: "0x2000000000000000000000000000000000000002",
				CommitmentCount:      2,
				DiscoveryBlock:       100,
			}},
			Limit:  req.Limit,
			Offset: req.Offset,
		}, nil
	}

	url := "/api/v1/data-anchor/daily-commitments?" +
		"factory_address=" + testDataAnchorFactoryAddress + "&" +
		"day_from=86400&day_to=172800&" +
		"institution_id=0x0000000000000000000000000000000000000000000000000000000000000001&" +
		"data_type=0x0000000000000000000000000000000000000000000000000000000000000002&" +
		"limit=10&offset=3"
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, url, nil)
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}

	var body struct {
		List   []api_storage.DailyCommitmentItem `json:"list"`
		Limit  int                               `json:"limit"`
		Offset int                               `json:"offset"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if len(body.List) != 1 || body.List[0].CommitmentCount != 2 ||
		body.Limit != 10 || body.Offset != 3 {
		t.Fatalf("unexpected response: %+v", body)
	}
}

func TestDailyCommitmentsRejectsUnsupportedMethod(t *testing.T) {
	t.Parallel()

	s := New(explorer.NewExplorer(), Config{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/data-anchor/daily-commitments", nil)
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
}

func TestDailyCommitmentsRejectsInvalidLimit(t *testing.T) {
	t.Parallel()

	s := New(explorer.NewExplorer(), Config{})
	s.getDailyCommitments = api_storage.GetDailyCommitments
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/data-anchor/daily-commitments?limit=101", nil)
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
}

func TestDailyCommitmentsRejectsMalformedFactoryAddress(t *testing.T) {
	t.Parallel()

	s := New(explorer.NewExplorer(), Config{})
	s.getDailyCommitments = api_storage.GetDailyCommitments
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/data-anchor/daily-commitments?factory_address=0x1", nil)
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}

	if !strings.Contains(rec.Body.String(), `"code":"invalid_address"`) {
		t.Fatalf("unexpected error response: %s", rec.Body.String())
	}
}

func TestDailyCommitmentsDatabaseErrorIsNotClassifiedByColumnName(t *testing.T) {
	t.Parallel()

	s := New(explorer.NewExplorer(), Config{})
	s.getDailyCommitments = func(api_storage.DailyCommitmentsRequest) (
		*api_storage.DailyCommitmentsResponse,
		error,
	) {
		return nil, errors.New(`pq: invalid input syntax for column "day_timestamp"`)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/data-anchor/daily-commitments", nil)
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}

	if strings.Contains(rec.Body.String(), "day_timestamp") {
		t.Fatalf("database error leaked in response: %s", rec.Body.String())
	}
}

func TestAdminDataAnchorFactoryDisabledWithoutSecret(t *testing.T) {
	t.Parallel()

	s := &Server{cfg: Config{}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/admin/v1/data-anchor/factories",
		strings.NewReader(`{"factory_address":"0x1","start_block":1}`))
	s.handleAdminDataAnchorFactories(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
}

func TestAdminDataAnchorFactoryRequiresAuthentication(t *testing.T) {
	t.Parallel()

	s := &Server{cfg: Config{AdminAPISecret: "secret"}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/admin/v1/data-anchor/factories",
		strings.NewReader(`{"factory_address":"0x1","start_block":1}`))
	s.handleAdminDataAnchorFactories(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
}

func TestAdminDataAnchorFactoryRejectsMalformedStartBlock(t *testing.T) {
	t.Parallel()

	tests := []string{
		`{"factory_address":"` + testDataAnchorFactoryAddress + `"}`,
		`{"factory_address":"` + testDataAnchorFactoryAddress + `",` +
			`"start_block":18446744073709551615}`,
	}
	for _, body := range tests {
		db, _, err := sqlmock.New()
		if err != nil {
			t.Fatalf("create sql mock: %v", err)
		}

		s := &Server{cfg: Config{DB: db, AdminAPISecret: "secret"}}
		rec := httptest.NewRecorder()
		s.handleAdminDataAnchorFactories(rec, newAdminDataAnchorRequest(body))

		_ = db.Close()

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status %d for %s: %s", rec.Code, body, rec.Body.String())
		}
	}
}

func TestAdminDataAnchorFactoryRejectsAddressWithoutBytecode(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sql mock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT EXISTS").
		WithArgs(testDataAnchorFactoryAddress).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			ID json.RawMessage `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode RPC request: %v", err)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      request.ID,
			"result":  "0x",
		})
	}))
	defer node.Close()

	s := &Server{cfg: Config{
		DB:             db,
		AdminAPISecret: "secret",
		NodeRPC:        node.URL,
	}}
	rec := httptest.NewRecorder()
	req := newAdminDataAnchorRequest(
		`{"factory_address":"` + testDataAnchorFactoryAddress + `","start_block":100}`)
	s.handleAdminDataAnchorFactories(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAdminDataAnchorFactoryUpsertsEnabledState(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sql mock: %v", err)
	}
	defer db.Close()

	address := testDataAnchorFactoryAddress
	mock.ExpectQuery("SELECT EXISTS").
		WithArgs(address).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery("INSERT INTO chain.data_anchor_factory_watchlist").
		WithArgs(address, uint64(100), false).
		WillReturnRows(sqlmock.NewRows([]string{"start_block", "next_block", "enabled"}).
			AddRow(100, 125, false))

	s := &Server{cfg: Config{DB: db, AdminAPISecret: "secret"}}
	rec := httptest.NewRecorder()
	req := newAdminDataAnchorRequest(
		`{"factory_address":"` + testDataAnchorFactoryAddress + `",` +
			`"start_block":100,"enabled":false}`)
	s.handleAdminDataAnchorFactories(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAdminDataAnchorFactoryRejectsStartBlockChangeAfterProcessing(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sql mock: %v", err)
	}
	defer db.Close()

	address := testDataAnchorFactoryAddress
	mock.ExpectQuery("SELECT EXISTS").
		WithArgs(address).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery("INSERT INTO chain.data_anchor_factory_watchlist").
		WithArgs(address, uint64(99), true).
		WillReturnRows(sqlmock.NewRows([]string{"start_block", "next_block", "enabled"}))

	s := &Server{cfg: Config{DB: db, AdminAPISecret: "secret"}}
	rec := httptest.NewRecorder()
	req := newAdminDataAnchorRequest(
		`{"factory_address":"` + testDataAnchorFactoryAddress + `","start_block":99}`)
	s.handleAdminDataAnchorFactories(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func newAdminDataAnchorRequest(body string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/admin/v1/data-anchor/factories",
		strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", "application/json")

	return req
}
