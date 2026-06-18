package api_storage

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/lib/pq"
)

type AssetIssuer struct {
	ID        string   `json:"id"`
	Name      string   `json:"name"`
	Website   string   `json:"website,omitempty"`
	Contact   string   `json:"contact,omitempty"`
	Assets    []string `json:"assets,omitempty"`
	Region    string   `json:"region,omitempty"`
	UpdatedAt string   `json:"updatedAt,omitempty"`
}

type AssetIssuerListResponse struct {
	Code    string        `json:"code"`
	Message string        `json:"message"`
	Data    []AssetIssuer `json:"data"`
}

func CreateAssetIssuer(issuer AssetIssuer) (string, error) {
	conn := getDB()
	if conn == nil {
		return "", errDBConnectionFailed
	}

	tx, err := conn.Begin()
	if err != nil {
		return "", fmt.Errorf("begin tx: %w", err)
	}

	defer tx.Rollback() //nolint:errcheck

	var id string

	err = tx.QueryRow(`
		INSERT INTO chain.asset_issuers (name, website, contact, region, updated_at)
		VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
		RETURNING id
	`, issuer.Name, nullIfEmpty(issuer.Website), nullIfEmpty(issuer.Contact),
		nullIfEmpty(issuer.Region)).Scan(&id)
	if err != nil {
		return "", fmt.Errorf("insert issuer: %w", err)
	}

	if err := linkTokens(tx, id, issuer.Assets); err != nil {
		return "", err
	}

	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("commit: %w", err)
	}

	return id, nil
}

func UpdateAssetIssuer(issuer AssetIssuer) error {
	conn := getDB()
	if conn == nil {
		return errDBConnectionFailed
	}

	tx, err := conn.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	defer tx.Rollback() //nolint:errcheck

	res, err := tx.Exec(`
		UPDATE chain.asset_issuers SET
			name = $1,
			website = $2,
			contact = $3,
			region = $4,
			updated_at = CURRENT_TIMESTAMP
		WHERE id = $5
	`, issuer.Name, nullIfEmpty(issuer.Website), nullIfEmpty(issuer.Contact),
		nullIfEmpty(issuer.Region), issuer.ID)
	if err != nil {
		return fmt.Errorf("update issuer: %w", err)
	}

	rows, _ := res.RowsAffected()
	if rows == 0 {
		return sql.ErrNoRows
	}

	if _, err := tx.Exec(`DELETE FROM chain.asset_issuer_tokens WHERE issuer_id = $1`, issuer.ID); err != nil {
		return fmt.Errorf("clear tokens: %w", err)
	}

	if err := linkTokens(tx, issuer.ID, issuer.Assets); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

func DeleteAssetIssuer(id string) error {
	conn := getDB()
	if conn == nil {
		return errDBConnectionFailed
	}

	res, err := conn.Exec(`DELETE FROM chain.asset_issuers WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete asset issuer: %w", err)
	}

	rows, _ := res.RowsAffected()
	if rows == 0 {
		return sql.ErrNoRows
	}

	return nil
}

func GetAssetIssuerList() (*AssetIssuerListResponse, error) {
	conn := getDB()
	if conn == nil {
		log.Printf("api_storage: database not configured")

		return &AssetIssuerListResponse{
			Code:    "500",
			Message: messageDBConnectionFailed,
		}, errDBConnectionFailed
	}

	rows, err := conn.Query(`
		SELECT i.id::text, i.name, COALESCE(i.website, ''), COALESCE(i.contact, ''),
		       COALESCE(i.region, ''), COALESCE(i.updated_at::text, ''),
		       COALESCE(array_agg(t.token_address) FILTER (WHERE t.token_address IS NOT NULL), '{}') AS assets
		FROM chain.asset_issuers i
		LEFT JOIN chain.asset_issuer_tokens t ON t.issuer_id = i.id
		GROUP BY i.id
		ORDER BY i.name ASC
	`)
	if err != nil {
		return &AssetIssuerListResponse{
			Code:    "500",
			Message: messageDBQueryFailed,
		}, err
	}

	defer rows.Close() //nolint:errcheck

	var list []AssetIssuer

	for rows.Next() {
		var issuer AssetIssuer

		var assets pq.StringArray

		if err := rows.Scan(&issuer.ID, &issuer.Name, &issuer.Website, &issuer.Contact,
			&issuer.Region, &issuer.UpdatedAt, &assets); err != nil {
			return &AssetIssuerListResponse{
				Code:    "500",
				Message: messageDBQueryFailed,
			}, err
		}

		issuer.Assets = []string(assets)

		list = append(list, issuer)
	}

	if err := rows.Err(); err != nil {
		return &AssetIssuerListResponse{
			Code:    "500",
			Message: messageDBQueryFailed,
		}, err
	}

	return &AssetIssuerListResponse{
		Code:    "200",
		Message: messageSuccess,
		Data:    list,
	}, nil
}

func linkTokens(tx *sql.Tx, issuerID string, assets []string) error {
	for _, addr := range assets {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}

		_, err := tx.Exec(`
			INSERT INTO chain.asset_issuer_tokens (issuer_id, token_address)
			VALUES ($1, $2)
		`, issuerID, addr)
		if err != nil {
			if strings.Contains(err.Error(), "violates foreign key") {
				return fmt.Errorf("token %s not found in watchlist", addr)
			}

			if strings.Contains(err.Error(), "violates unique constraint") {
				return fmt.Errorf("token %s already assigned to another issuer", addr)
			}

			return fmt.Errorf("link token %s: %w", addr, err)
		}
	}

	return nil
}

func nullIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}

	return s
}
