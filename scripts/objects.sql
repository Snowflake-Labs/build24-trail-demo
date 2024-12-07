use role accountadmin;

-- alerts
grant execute alert on account to role kamesh_demos; -- noqa: 
-- serverless alerts
grant execute managed alert on account to role kamesh_demos;
-- create  
grant  create integration on account to role kamesh_demos;

use role kamesh_demos;
use database kamesh_demos;
-- schema to hold all tables, functions, stored procedures
create schema if not exists data;
-- stage for udf function files, other data files
create schema if not exists stages;
-- place for all Python UDF/Sproc sources
create schema if not exists src;
-- place for all tasks
create schema if not exists tasks;
-- place for all alerts, notifications and their related secrets
create schema if not exists alerts_and_notification;

select
    truck_id,
    review,
    SNOWFLAKE.CORTEX.SENTIMENT(review) as sentiment_score
from analytics.truck_reviews_v
where date_part('year', date) = 2024 limit 5;

select * from data.truck_review_sentiments limit 5;
