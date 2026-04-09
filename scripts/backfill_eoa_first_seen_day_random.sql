-- Dev/UI helper: synthetic chain.eoa_first_seen rows (random "new EOA" counts per UTC hour).
-- Same anchor + backward hour range as dev_duplicate_erc20_daily_stats_forward.sql and
-- backfill_entity_participation.sql.
--
-- Anchor = LEAST( max(first_seen_hour_utc) in table , current UTC hour ). Inserts for NUM_HOURS strictly before anchor.
-- Each row is a unique pseudo-address; first_seen_hour_utc is that EOA's first-seen hour (onboarding series).
--
-- Usage:
--   TRUNCATE chain.eoa_first_seen;
--   psql "$DATABASE_URL" -f scripts/backfill_eoa_first_seen_day_random.sql

WITH
  utc_hour AS (SELECT date_trunc('hour', (CURRENT_TIMESTAMP AT TIME ZONE 'utc')) AS h),
  anchor AS (
    SELECT LEAST(
      COALESCE((SELECT MAX(first_seen_hour_utc) FROM chain.eoa_first_seen), (SELECT h FROM utc_hour)),
      (SELECT h FROM utc_hour)
    ) AS h
  ),
  num_hours AS (SELECT 744 AS n), -- 31 × 24h (~full calendar month), same as dev_duplicate_erc20_daily_stats_forward.sql
  hours AS (
    SELECT generate_series(1, (SELECT n FROM num_hours)) AS i
  ),
  hour_list AS (
    SELECT (a.h - (hours.i * interval '1 hour')) AS first_seen_hour_utc
    FROM anchor a
    CROSS JOIN hours
  )
INSERT INTO chain.eoa_first_seen (address, first_seen_hour_utc)
SELECT
  -- 0x + 40 hex chars (varchar(42)); md5 is 32 chars — take first 40 of two concatenated hashes.
  '0x' || substr(
    md5('eoa' || hl.first_seen_hour_utc::text || gs.i::text || random()::text || clock_timestamp()::text)
    || md5('eoa' || gs.i::text || hl.first_seen_hour_utc::text || random()::text),
    1,
    40
  ),
  hl.first_seen_hour_utc
FROM hour_list hl
CROSS JOIN generate_series(1, 5) AS gs(i);
