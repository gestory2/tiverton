------------------------------------------------------------------------------------------------------------------------------------
-- SETUP
------------------------------------------------------------------------------------------------------------------------------------

-- create the internal stage to store the raw files
CREATE OR REPLACE STAGE tdp_dev.bronze.stg_ag_data
	DIRECTORY = ( ENABLE = TRUE );

-- create a file format to read the csv files
CREATE OR REPLACE FILE FORMAT tdp_dev.bronze.ff_csv
	TYPE = CSV
	SKIP_HEADER = 1;

-- create a file format to read the json files
CREATE OR REPLACE FILE FORMAT tdp_dev.bronze.ff_json
	TYPE = JSON
	STRIP_OUTER_ARRAY = TRUE;

-- create the borrower table
CREATE OR REPLACE TABLE tdp_dev.bronze.borrowers (
	borrower_id VARCHAR,
	borrower_name VARCHAR,
	primary_county_fips VARCHAR,
	primary_crop VARCHAR,
	loan_balance NUMBER,
	origination_date DATE
);

-- create the weather table
CREATE OR REPLACE TABLE tdp_dev.bronze.weather_observations (
	observation_date DATE,
	county_fips VARCHAR,
	precip_inches FLOAT,
	temp_max_f FLOAT,
	temp_min_f FLOAT,
	drought_index FLOAT
);

-- create the loss history table
-- store the json as a variant, will put a view on top later for easier consumption
CREATE OR REPLACE TABLE tdp_dev.bronze.rma_loss_history_raw (
	payload VARIANT
);

-- next, manually upload each of the three files to its own dedicated directory in the stage: stg_ag_data/{table_name}
-- in a practical application, we would have an ETL process acquiring and saving the data to the stage automatically
-- each incoming file would be saved to its dedicated directory, then merged or truncated and reloaded according to the requirement

------------------------------------------------------------------------------------------------------------------------------------
-- ETL
------------------------------------------------------------------------------------------------------------------------------------

-- copy the data from the borrower csv into its corresponding table, ignoring error rows
COPY INTO tdp_dev.bronze.borrowers
FROM @tdp_dev.bronze.stg_ag_data/borrowers
FILE_FORMAT = tdp_dev.bronze.ff_csv
ON_ERROR = 'CONTINUE';

-- copy the data from the weather csv into its corresponding table, ignoring error rows
COPY INTO tdp_dev.bronze.weather_observations
FROM @tdp_dev.bronze.stg_ag_data/weather_observations
FILE_FORMAT = tdp_dev.bronze.ff_csv
ON_ERROR = 'CONTINUE';

-- copy the records from the loss history json into the raw table, ignoring error rows 
COPY INTO tdp_dev.bronze.rma_loss_history_raw
FROM @tdp_dev.bronze.stg_ag_data/rma_loss_history
FILE_FORMAT = tdp_dev.bronze.ff_json
ON_ERROR = 'CONTINUE';

-- create a view to parse the json for easier consumption
-- flatten not needed since indemnity_details is a dictionary, not an array
CREATE OR REPLACE VIEW tdp_dev.bronze.rma_loss_history AS
SELECT
	r.payload:county_fips::VARCHAR AS county_fips,
	r.payload:crop_year::NUMBER AS crop_year,
	r.payload:crop::VARCHAR AS crop,
	r.payload:indemnity_details:cause_of_loss::VARCHAR AS cause_of_loss,
	r.payload:indemnity_details:indemnity_amount::NUMBER AS indemnity_amount,
	r.payload:indemnity_details:net_acres::NUMBER AS net_acres,
	r.payload:indemnity_details:loss_date::DATE AS loss_date
FROM
	tdp_dev.bronze.rma_loss_history_raw AS r;
