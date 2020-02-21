CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.nondesktop_clients_last_seen_v1` AS
SELECT
  submission_date,
  client_id,
  days_seen_bits,
  days_since_seen,
  days_created_profile_bits,
  days_since_created_profile,
  app_name,
  os,
  osversion AS os_version,
  normalized_channel,
  campaign,
  country,
  locale,
  distribution_id,
  metadata_app_version AS app_version
FROM
  `moz-fx-data-shared-prod.telemetry.core_clients_last_seen`
  --
UNION ALL
  --
SELECT
  submission_date,
  client_id,
  days_seen_bits,
  days_since_seen,
  1 << days_since_created_profile AS days_created_profile_bits,
  days_since_created_profile,
  'Fenix' AS app_name,
  os,
  os_version,
  normalized_channel,
  NULL AS campaign,
  country,
  locale,
  NULL AS distribution_id,
  app_display_version AS app_version
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox.clients_last_seen`
  --
UNION ALL
  --
SELECT
  submission_date,
  client_id,
  days_seen_bits,
  days_since_seen,
  1 << days_since_created_profile AS days_created_profile_bits,
  days_since_created_profile,
  'FirefoxReality' AS app_name,
  os,
  os_version,
  normalized_channel,
  NULL AS campaign,
  country,
  locale,
  distribution_channel_name AS distribution_id,
  app_display_version AS app_version
FROM
  `moz-fx-data-shared-prod.org_mozilla_vrbrowser.clients_last_seen`
