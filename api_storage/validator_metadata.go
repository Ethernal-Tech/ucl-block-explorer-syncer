package api_storage

import (
	"database/sql"
	"fmt"
	"log"
)

type ValidatorMetadata struct {
	Address     string `json:"address"`
	Name        string `json:"name,omitempty"`
	Institution string `json:"institution,omitempty"`
	Region      string `json:"region,omitempty"`
	UpdatedAt   string `json:"updatedAt,omitempty"`
}

type ValidatorMetadataListResponse struct {
	Code    string              `json:"code"`
	Message string              `json:"message"`
	Data    []ValidatorMetadata `json:"data"`
}

func UpsertValidatorMetadata(m ValidatorMetadata) error {
	conn := getDB()
	if conn == nil {
		return errDBConnectionFailed
	}

	_, err := conn.Exec(`
		INSERT INTO chain.validator_metadata (address, name, institution, region, updated_at)
		VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
		ON CONFLICT (address) DO UPDATE SET
			name = EXCLUDED.name,
			institution = EXCLUDED.institution,
			region = EXCLUDED.region,
			updated_at = CURRENT_TIMESTAMP
	`, m.Address, m.Name, m.Institution, m.Region)
	if err != nil {
		return fmt.Errorf("upsert validator metadata: %w", err)
	}

	return nil
}

func DeleteValidatorMetadata(address string) error {
	conn := getDB()
	if conn == nil {
		return errDBConnectionFailed
	}

	res, err := conn.Exec(`DELETE FROM chain.validator_metadata WHERE address = $1`, address)
	if err != nil {
		return fmt.Errorf("delete validator metadata: %w", err)
	}

	rows, _ := res.RowsAffected()
	if rows == 0 {
		return sql.ErrNoRows
	}

	return nil
}

func GetValidatorMetadataList() (*ValidatorMetadataListResponse, error) {
	conn := getDB()
	if conn == nil {
		log.Printf("api_storage: database not configured")

		return &ValidatorMetadataListResponse{
			Code:    "500",
			Message: messageDBConnectionFailed,
		}, errDBConnectionFailed
	}

	rows, err := conn.Query(`
		SELECT address, COALESCE(name, ''), COALESCE(institution, ''), COALESCE(region, ''), 
		       COALESCE(updated_at::text, '')
		FROM chain.validator_metadata
		ORDER BY address ASC
	`)
	if err != nil {
		return &ValidatorMetadataListResponse{
			Code:    "500",
			Message: messageDBQueryFailed,
		}, err
	}

	defer rows.Close() //nolint:errcheck

	var list []ValidatorMetadata

	for rows.Next() {
		var v ValidatorMetadata

		if err := rows.Scan(&v.Address, &v.Name, &v.Institution, &v.Region, &v.UpdatedAt); err != nil {
			return &ValidatorMetadataListResponse{
				Code:    "500",
				Message: messageDBQueryFailed,
			}, err
		}

		list = append(list, v)
	}

	if err := rows.Err(); err != nil {
		return &ValidatorMetadataListResponse{
			Code:    "500",
			Message: messageDBQueryFailed,
		}, err
	}

	return &ValidatorMetadataListResponse{
		Code:    "200",
		Message: messageSuccess,
		Data:    list,
	}, nil
}
