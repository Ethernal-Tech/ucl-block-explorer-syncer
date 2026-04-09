-- Dev/UI helper: copy metrics from the latest row in chain.erc20_hourly_stats onto
-- previous UTC hours (same tokens), for stretching charts without reindexing.
--
-- Anchor = LEAST( max(hour_utc) in DB , current UTC hour ). Inserts for NUM_HOURS strictly before anchor.
--
-- Usage:
--   psql "$DATABASE_URL" -f scripts/dev_duplicate_erc20_daily_stats_forward.sql
--
-- Clean up future-dated noise if needed:
--   DELETE FROM chain.erc20_hourly_stats
--   WHERE hour_utc > date_trunc('hour', (CURRENT_TIMESTAMP AT TIME ZONE 'utc'));
--
-- Idempotent: ON CONFLICT DO NOTHING skips (token_address, hour_utc) that already exist.

WITH
  utc_hour AS (SELECT date_trunc('hour', (CURRENT_TIMESTAMP AT TIME ZONE 'utc')) AS h),
  anchor AS (
    SELECT LEAST(
      COALESCE((SELECT MAX(hour_utc) FROM chain.erc20_hourly_stats), (SELECT h FROM utc_hour)),
      (SELECT h FROM utc_hour)
    ) AS h
  ),
  num_hours AS (SELECT 744 AS n), -- 31 × 24h (~full calendar month)
  series AS (SELECT generate_series(1, (SELECT n FROM num_hours)) AS i)
INSERT INTO chain.erc20_hourly_stats (
    token_address,
    hour_utc,
    transfer_count,
    transfer_volume_raw,
    mint_count,
    mint_volume_raw,
    burn_count,
    burn_volume_raw
  )
SELECT
  s.token_address,
  a.h - (ser.i * interval '1 hour'),
  s.transfer_count,
  s.transfer_volume_raw,
  s.mint_count,
  s.mint_volume_raw,
  s.burn_count,
  s.burn_volume_raw
FROM chain.erc20_hourly_stats s
CROSS JOIN anchor a
CROSS JOIN series ser
WHERE s.hour_utc = (SELECT MAX(hour_utc) FROM chain.erc20_hourly_stats)
ON CONFLICT (token_address, hour_utc) DO NOTHING;
