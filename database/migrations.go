package database

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"

	"github.com/golang-migrate/migrate/v4"
	migratepostgres "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

// migrationFiles is bundled into the syncer binary so deployments do not need
// migration files mounted alongside the executable.
//
//go:embed migrations/*.sql
var migrationFiles embed.FS

type LogFunc func(format string, args ...any)

// RunMigrations applies all pending migrations using PostgreSQL's advisory
// migration lock. The API command deliberately does not call this function;
// sync owns schema upgrades. Transaction list/by-hash queries degrade
// is_data_anchor to false until these tables exist.
func RunMigrations(db *sql.DB, logf LogFunc) error {
	if db == nil {
		return fmt.Errorf("database is required")
	}

	if _, err := db.Exec(`CREATE SCHEMA IF NOT EXISTS chain`); err != nil {
		return fmt.Errorf("ensure migration schema: %w", err)
	}

	sourceFS, err := fs.Sub(migrationFiles, "migrations")
	if err != nil {
		return fmt.Errorf("open embedded migrations: %w", err)
	}

	sourceDriver, err := iofs.New(sourceFS, ".")
	if err != nil {
		return fmt.Errorf("create migration source: %w", err)
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		_ = sourceDriver.Close()

		return fmt.Errorf("acquire migration connection: %w", err)
	}

	databaseDriver, err := migratepostgres.WithConnection(context.Background(), conn, &migratepostgres.Config{
		SchemaName:      "chain",
		MigrationsTable: "schema_migrations",
	})
	if err != nil {
		_ = conn.Close()
		_ = sourceDriver.Close()

		return fmt.Errorf("create postgres migration driver: %w", err)
	}

	runner, err := migrate.NewWithInstance("iofs", sourceDriver, "postgres", databaseDriver)
	if err != nil {
		_ = databaseDriver.Close()
		_ = sourceDriver.Close()

		return fmt.Errorf("create migration runner: %w", err)
	}

	defer func() {
		_, _ = runner.Close()
	}()

	startVersion, dirty, err := migrationVersion(runner)
	if err != nil {
		return err
	}

	if logf != nil {
		logf("database schema migration starting at version %d (dirty=%t)", startVersion, dirty)
	}

	if err := runner.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("apply database migrations: %w", err)
	}

	resultVersion, dirty, err := migrationVersion(runner)
	if err != nil {
		return err
	}

	if dirty {
		return fmt.Errorf("database migration version %d is dirty after migration", resultVersion)
	}

	if logf != nil {
		logf("database schema migration completed at version %d", resultVersion)
	}

	return nil
}

func migrationVersion(runner *migrate.Migrate) (uint, bool, error) {
	version, dirty, err := runner.Version()
	if errors.Is(err, migrate.ErrNilVersion) {
		return 0, false, nil
	}

	if err != nil {
		return 0, false, fmt.Errorf("read database migration version: %w", err)
	}

	return version, dirty, nil
}
