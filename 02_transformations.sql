------------------------------------------------------------------------------------------------------------------------------------
-- dim_borrowers
------------------------------------------------------------------------------------------------------------------------------------

-- a view is much easier here since the datediff is always changing. dynamic tables could also be used
CREATE VIEW tdp_dev.silver.dim_borrowers AS
SELECT
	b.borrower_id,
	b.borrower_name,
	b.primary_county_fips,
	b.primary_crop,
	b.loan_balance,
	b.origination_date,
	DATEDIFF(DAY, origination_date, CURRENT_DATE()) AS days_since_origination,
	CASE
		WHEN b.loan_balance < 5000000 THEN 'Small'
		WHEN b.loan_balance BETWEEN 5000000 AND 10000000 THEN 'Medium'
		WHEN b.loan_balance > 10000000 THEN 'Large'
		ELSE 'Other'
	END AS loan_size_tier
FROM
	tdp_dev.bronze.borrowers AS b;

------------------------------------------------------------------------------------------------------------------------------------
-- fct_county_risk_monthly
------------------------------------------------------------------------------------------------------------------------------------

-- data is not large, so a view is simpler than persisting to a table. use CTEs to build piece by piece
-- use net_acres as the denominator in the loss ratio since it is the best proxy we have for exposure
-- doesn't make sense to have columns for loss counts and amounts by crop and cause since the values in those fields are dynamic
-- if that information is needed, this table would need to be expanded to that grain, or separate fact tables could be created
CREATE VIEW tdp_dev.silver.fct_county_risk_monthly AS
WITH
loss_history_county_month AS (
	SELECT
		lh.county_fips,
		YEAR(lh.loss_date) AS loss_year,
		MONTH(lh.loss_date) AS loss_month,
		SUM(lh.net_acres) AS net_acres,
		SUM(lh.indemnity_amount) AS indemnity_amount,
		COUNT(*) loss_count
	FROM
		tdp_dev.bronze.rma_loss_history AS lh
	GROUP BY
		lh.county_fips,
		YEAR(lh.loss_date),
		MONTH(lh.loss_date)
),
weather_county_month AS (
	SELECT
		wo.county_fips,
		YEAR(wo.observation_date) AS observation_year,
		MONTH(wo.observation_date) AS observation_month,
		SUM(wo.precip_inches) AS precip_inches,
		AVG(wo.temp_max_f) AS avg_high
	FROM
		tdp_dev.bronze.weather_observations AS wo
	GROUP BY
		wo.county_fips,
		YEAR(wo.observation_date),
		MONTH(wo.observation_date)
),
weather_county_month_with_moving_averages AS (
	SELECT
		wcm.county_fips,
		wcm.observation_year,
		wcm.observation_month,
		wcm.precip_inches,
		wcm.avg_high,
		AVG(wcm.precip_inches) OVER (PARTITION BY wcm.county_fips, wcm.observation_month ORDER BY wcm.observation_year ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS mavg_precip,
		STDDEV_POP(wcm.precip_inches) OVER (PARTITION BY wcm.county_fips, wcm.observation_month ORDER BY wcm.observation_year ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS mstd_precip,
		AVG(wcm.avg_high) OVER (PARTITION BY wcm.county_fips, wcm.observation_month ORDER BY wcm.observation_year ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS mavg_high,
		STDDEV_POP(wcm.avg_high) OVER (PARTITION BY wcm.county_fips, wcm.observation_month ORDER BY wcm.observation_year ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS mstd_high
	FROM
		weather_county_month AS wcm
	ORDER BY
		wcm.county_fips,
		wcm.observation_year,
		wcm.observation_month
)
SELECT
	wcmma.county_fips AS county,
	wcmma.observation_year AS year,
	wcmma.observation_month AS month,
	wcmma.precip_inches,
	wcmma.avg_high,
	CASE
		WHEN ABS(wcmma.precip_inches - wcmma.mavg_precip) > (1.5 * wcmma.mstd_precip) THEN 1
		ELSE 0
	END AS precip_anomaly_flag,
	CASE
		WHEN ABS(wcmma.avg_high - wcmma.mavg_high) > (1.5 * wcmma.mstd_high) THEN 1
		ELSE 0
	END AS temp_anomaly_flag,
	lhcm.net_acres,
	lhcm.indemnity_amount,
	CASE
		WHEN lhcm.net_acres = 0 THEN NULL
		ELSE lhcm.indemnity_amount / lhcm.net_acres
	END loss_ratio
FROM
	weather_county_month_with_moving_averages AS wcmma
	LEFT JOIN loss_history_county_month AS lhcm
		ON wcmma.county_fips = lhcm.county_fips AND wcmma.observation_year = lhcm.loss_year AND wcmma.observation_month = lhcm.loss_month
ORDER BY
	wcmma.county_fips,
	wcmma.observation_year,
	wcmma.observation_month;

------------------------------------------------------------------------------------------------------------------------------------
-- fct_borrower_risk_score
------------------------------------------------------------------------------------------------------------------------------------

-- ran out of time for this one
