use role accountadmin;
grant EXECUTE TASK on account to role kamesh_demos;
use role kamesh_demos;
USE DATABASE kamesh_demos;
-- schema to hold all tables, functions, stored procedures
CREATE SCHEMA IF NOT EXISTS data;
-- stage for udf function files, other data files
CREATE SCHEMA IF NOT EXISTS stages;

SELECT
    truck_id,
    review,
    SNOWFLAKE.CORTEX.SENTIMENT(review) AS sentiment_score
FROM analytics.truck_reviews_v
WHERE date_part('year', date) = 2024 limit 5;


select * from data.truck_review_sentiments limit 5;

drop table data.truck_review_sentiments;
