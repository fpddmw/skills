# SQLite Schema

## 1) `aq_raw_observations`
Purpose:
- Persist raw OpenAQ observations (source payload preserved).

Idempotency key:
- `(source_name, sensor_id, parameter_code, observed_utc)`

Key columns:
- spatial/context: `location_id`, `location_name`, `country_code`, `latitude`, `longitude`
- measurement: `parameter_code`, `observed_utc`, `value_raw`, `unit_raw`
- trace: `payload_json`

## 2) `aq_enriched_observations`
Purpose:
- Store normalized concentration, chosen standard profile, and exceedance metrics.

Idempotency key:
- `raw_id` unique (1 enriched row per raw row)

Key columns:
- `standard_profile` (`who_2021`, `us_epa_core`)
- `threshold_value`, `threshold_unit`, `threshold_ugm3`
- `value_ugm3`, `variance_ratio`, `is_exceed`
- `note` (conversion/missing-data notes)

## 3) `physical_metrics`
Purpose:
- Daily aggregate summary for observer-layer downstream use.

Idempotency key:
- `metric_key = metric_date|country_code|parameter_code|standard_profile`

Key columns:
- `metric_date`, `country_code`, `parameter_code`, `standard_profile`
- `sample_count`, `avg_value_ugm3`, `max_value_ugm3`
- `exceed_count`, `exceed_rate`, `max_variance_ratio`
- `source_row_count`
