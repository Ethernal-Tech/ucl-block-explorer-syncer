-- Dev/UI helper: synthetic chain.entity_hour_participation rows with random addresses across many
-- UTC hours (same anchor pattern as dev_duplicate_erc20_daily_stats_forward.sql).
--
-- Anchor = LEAST( max(hour_utc) in table , current UTC hour ). Rows are inserted for the NUM_HOURS
-- hours strictly before anchor (one batch per run).
--
-- Usage:
--   TRUNCATE chain.entity_hour_participation;
--   psql "$DATABASE_URL" -f scripts/backfill_entity_participation.sql
--
-- Idempotent for (hour_utc, address): duplicate random addresses same hour are skipped by DISTINCT.

WITH
  utc_hour AS (SELECT date_trunc('hour', (CURRENT_TIMESTAMP AT TIME ZONE 'utc')) AS h),
  anchor AS (
    SELECT LEAST(
      COALESCE((SELECT MAX(hour_utc) FROM chain.entity_hour_participation), (SELECT h FROM utc_hour)),
      (SELECT h FROM utc_hour)
    ) AS h
  ),
  num_hours AS (SELECT 744 AS n), -- 31 × 24h (~full calendar month), same as dev_duplicate_erc20_daily_stats_forward.sql
  hours AS (
    SELECT generate_series(1, (SELECT n FROM num_hours)) AS i
  ),
  hour_list AS (
    SELECT (a.h - (hours.i * interval '1 hour')) AS hour_utc
    FROM anchor a
    CROSS JOIN hours
  )
INSERT INTO chain.entity_hour_participation (hour_utc, address)
SELECT DISTINCT
  hl.hour_utc,
  '0x' || substr(
    md5(hl.hour_utc::text || gs.i::text || random()::text || clock_timestamp()::text)
    || md5(gs.i::text || hl.hour_utc::text || random()::text),
    1,
    40
  )
FROM hour_list hl
CROSS JOIN generate_series(1, 12) AS gs(i);
