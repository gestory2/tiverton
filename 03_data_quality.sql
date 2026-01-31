------------------------------------------------------------------------------------------------------------------------------------
-- Data Quality Checks
------------------------------------------------------------------------------------------------------------------------------------

-- check for duplicate borrower ids
-- no duplicates here
SELECT
	db.borrower_id,
	COUNT(*)
FROM
	tdp_dev.silver.dim_borrowers AS db
GROUP BY
	db.borrower_id
HAVING
	COUNT(*) > 1

-- check for borrowers with counties missing from weather data
-- there's 25 missing counties affecting 26 borrowers
SELECT
	COUNT(*) AS borrower_count,
	COUNT(DISTINCT b.primary_county_fips) AS counties_missing
FROM
	tdp_dev.bronze.borrowers AS b
	LEFT JOIN tdp_dev.bronze.weather_observations AS wo
		ON b.primary_county_fips = wo.county_fips
WHERE
	wo.county_fips IS NULL

-- check for negative or zero loan balance
-- all balances are positive
SELECT
	COUNT(*)
FROM
	tdp_dev.bronze.borrowers AS b
WHERE
	b.loan_balance <= 0

-- check for negative loss amounts
-- no negative amounts here
SELECT
	COUNT(*)
FROM
	tdp_dev.bronze.rma_loss_history AS rlh
WHERE
	rlh.indemnity_amount < 0

-- check for nulls in both temperature columns
-- no nulls here
SELECT
	COUNT(*)
FROM
	tdp_dev.bronze.weather_observations AS wo
WHERE
	wo.temp_min_f IS NULL OR
	wo.temp_max_f IS NULL

-- check weather data freshness
-- last data point is from 12/31/2024
SELECT
	MAX(wo.observation_date) AS last_observation_date,
	DATEDIFF(DAY, MAX(wo.observation_date), CURRENT_DATE()) days_since_last_observation
FROM
	tdp_dev.bronze.weather_observations AS wo
