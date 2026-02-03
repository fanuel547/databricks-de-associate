CREATE OR REPLACE TABLE de_associate.pipeline_runs_log(
  run_id BIGINT GENERATED ALWAYS AS IDENTITY,
  run_date TIMESTAMP,
  run_message STRING,
  run_status STRING
) USING DELTA;