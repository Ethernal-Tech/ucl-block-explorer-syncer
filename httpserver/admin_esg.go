package httpserver

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type esgResponse struct {
	Time                    time.Time `json:"time"`
	TotalLMBCarbonEmissions float64   `json:"total_lmb_carbon_emissions"`
	TotalMBMCarbonEmissions float64   `json:"total_mbm_carbon_emissions"`
}

func (s *Server) handleAdminEsg(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if s.cfg.AdminAPISecret == "" {
		writeError(w, http.StatusNotFound, "admin API disabled")

		return
	}

	token := parseBearerToken(r)
	if token == "" || !constantTimeEqualString(token, s.cfg.AdminAPISecret) {
		writeError(w, http.StatusUnauthorized, "unauthorized")

		return
	}

	if s.cfg.DB == nil {
		writeError(w, http.StatusServiceUnavailable, "database not configured")

		return
	}

	writeError := func(status int, msg string) {
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
	}
	getQueryParamInt := func(q url.Values, name string, def int) (int, error) {
		s := q.Get(name)
		if s == "" {
			return def, nil
		}

		return strconv.Atoi(s)
	}
	now := time.Now().UTC()

	switch r.Method {
	case http.MethodGet:
		// Parse optional ESG range query parameters from URL: from_year, from_month, to_year, to_month
		q := r.URL.Query()

		fromYear, err := getQueryParamInt(q, "from_year", now.Year())
		if err != nil {
			writeError(http.StatusBadRequest, "invalid from_year")

			return
		}

		fromMonth, err := getQueryParamInt(q, "from_month", int(now.Month()))
		if err != nil {
			writeError(http.StatusBadRequest, "invalid from_month")

			return
		}

		toYear, err := getQueryParamInt(q, "to_year", now.Year())
		if err != nil {
			writeError(http.StatusBadRequest, "invalid to_year")

			return
		}

		toMonth, err := getQueryParamInt(q, "to_month", int(now.Month()))
		if err != nil {
			writeError(http.StatusBadRequest, "invalid to_month")

			return
		}

		if err := writeEsgData(s.cfg.DB, w, fromYear, fromMonth, toYear, toMonth); err != nil {
			writeError(http.StatusInternalServerError, "failed to retrieve esg data")

			return
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
	}
}

func writeEsgData(
	db *sql.DB, w http.ResponseWriter, fromYear, fromMonth, toYear, toMonth int,
) error {
	fromTime := time.Date(fromYear, time.Month(fromMonth), 1, 0, 0, 0, 0, time.UTC)
	// toTime = end of given month
	toTime := time.Date(toYear, time.Month(toMonth), 1, 0, 0, 0, 0, time.UTC).AddDate(0, 1, 0).Add(-time.Nanosecond)

	rows, err := db.Query(`
			SELECT time_at, total_lbm_carbon_emissions, total_mbm_carbon_emissions
			FROM chain.esg_state
			WHERE time_at BETWEEN $1 AND $2
			ORDER BY time_at ASC
		`, fromTime, toTime)
	if err != nil {
		return fmt.Errorf("failed to retrieve esg data from db: %w", err)
	}

	defer rows.Close() //nolint:errcheck

	data := []esgResponse{}

	for rows.Next() {
		var r esgResponse

		if err := rows.Scan(&r.Time, &r.TotalLMBCarbonEmissions, &r.TotalMBMCarbonEmissions); err != nil {
			return fmt.Errorf("failed to scan esg data row: %w", err)
		}

		data = append(data, r)
	}

	resp := map[string]any{
		"data": data,
	}

	return json.NewEncoder(w).Encode(resp)
}
