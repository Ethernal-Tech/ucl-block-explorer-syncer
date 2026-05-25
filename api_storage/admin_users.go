package api_storage

import (
	"database/sql"
	"errors"
	"fmt"

	"golang.org/x/crypto/bcrypt"
)

func CrateAdminUser(username, password string) error {
	conn := getDB()
	if conn == nil {
		return errors.New("database connection failed")
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("hash password: %w", err)
	}

	_, err = conn.Exec(`
		INSERT INTO chain.admin_users (username, password_hash)
		VALUES ($1, $2)
		ON CONFLICT (username) DO UPDATE SET
			password_hash = EXCLUDED.password_hash
	`, username, string(hash))
	if err != nil {
		return fmt.Errorf("create admin user: %w", err)
	}

	return nil
}

func AuthenticateAdmin(username, password string) (bool, error) {
	conn := getDB()
	if conn == nil {
		return false, errors.New("database connection failed")
	}

	var hash string

	err := conn.QueryRow(
		`SELECT password_hash FROM chain.admin_users WHERE username = $1`,
		username,
	).Scan(&hash)
	if err != nil {
		return false, nil
	}

	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)); err != nil {
		return false, nil
	}

	return true, nil
}

type AdminUser struct {
	ID        string `json:"id"`
	Username  string `json:"username"`
	CreatedAt string `json:"createdAt"`
}

type AdminUserListResponse struct {
	Code    string      `json:"code"`
	Message string      `json:"message"`
	Data    []AdminUser `json:"data"`
}

func GetAdminUserList() (*AdminUserListResponse, error) {
	conn := getDB()
	if conn == nil {
		return nil, errors.New("database connection failed")
	}

	rows, err := conn.Query(`
		SELECT id::text, username, COALESCE(created_at::text, '')
		FROM chain.admin_users
		ORDER BY username ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck

	var list []AdminUser

	for rows.Next() {
		var u AdminUser

		if err := rows.Scan(&u.ID, &u.Username, &u.CreatedAt); err != nil {
			return nil, err
		}

		list = append(list, u)
	}

	return &AdminUserListResponse{
		Code:    "200",
		Message: "Success",
		Data:    list,
	}, nil
}

func UpdateAdminUser(id, username, currentPassword, newPassword string) error {
	conn := getDB()
	if conn == nil {
		return errors.New("database connection failed")
	}

	// Verify current password
	var existingHash string

	err := conn.QueryRow(
		`SELECT password_hash FROM chain.admin_users WHERE id = $1`, id,
	).Scan(&existingHash)
	if err != nil {
		return sql.ErrNoRows
	}

	if err := bcrypt.CompareHashAndPassword([]byte(existingHash), []byte(currentPassword)); err != nil {
		return fmt.Errorf("current password is incorrect")
	}

	// Update
	if newPassword != "" {
		hash, err := bcrypt.GenerateFromPassword([]byte(newPassword), bcrypt.DefaultCost)
		if err != nil {
			return fmt.Errorf("hash password: %w", err)
		}

		_, err = conn.Exec(`
			UPDATE chain.admin_users SET username = $1, password_hash = $2 WHERE id = $3
		`, username, string(hash), id)

		return err
	}

	_, err = conn.Exec(`
		UPDATE chain.admin_users SET username = $1 WHERE id = $2
	`, username, id)

	return err
}

func DeleteAdminUser(id string) error {
	conn := getDB()
	if conn == nil {
		return errors.New("database connection failed")
	}

	res, err := conn.Exec(`DELETE FROM chain.admin_users WHERE id = $1`, id)
	if err != nil {
		return err
	}

	rows, _ := res.RowsAffected()
	if rows == 0 {
		return sql.ErrNoRows
	}

	return nil
}
