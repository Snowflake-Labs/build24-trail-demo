USE ROLE <%ctx.env.SNOWFLAKE_ROLE%>;

/*--
• database, schema and warehouse creation
--*/

-- create <%ctx.env.SNOWFLAKE_DATABASE%> database
CREATE DATABASE IF NOT EXISTS  <%ctx.env.SNOWFLAKE_DATABASE%>;

-- create raw_pos schema
CREATE OR REPLACE SCHEMA <%ctx.env.SNOWFLAKE_DATABASE%>.raw_pos;

-- create raw_customer schema
CREATE OR REPLACE SCHEMA <%ctx.env.SNOWFLAKE_DATABASE%>.raw_support;

-- create harmonized schema
CREATE OR REPLACE SCHEMA <%ctx.env.SNOWFLAKE_DATABASE%>.harmonized;

-- create analytics schema
CREATE OR REPLACE SCHEMA <%ctx.env.SNOWFLAKE_DATABASE%>.analytics;

-- create tasty_ds_wh warehouse
CREATE OR REPLACE WAREHOUSE tasty_ds_wh
WAREHOUSE_SIZE = 'large'
WAREHOUSE_TYPE = 'standard'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE
INITIALLY_SUSPENDED = TRUE
COMMENT = 'data science warehouse for tasty bytes';


USE WAREHOUSE tasty_ds_wh;

/*--
• file format and stage creation
--*/

CREATE OR REPLACE FILE FORMAT <%ctx.env.SNOWFLAKE_DATABASE%>.public.csv_ff 
TYPE = 'csv';

CREATE OR REPLACE STAGE <%ctx.env.SNOWFLAKE_DATABASE%>.public.s3load
    COMMENT = 'Quickstarts S3 Stage Connection'
    URL = 's3://sfquickstarts/tastybytes-voc/'
    FILE_FORMAT = <%ctx.env.SNOWFLAKE_DATABASE%>.public.csv_ff;

/*--
raw zone table build 
--*/

-- menu table build
CREATE OR REPLACE TABLE <%ctx.env.SNOWFLAKE_DATABASE%>.raw_pos.menu
(
    menu_id NUMBER(19,0),
    menu_type_id NUMBER(38,0),
    menu_type VARCHAR(16777216),
    truck_brand_name VARCHAR(16777216),
    menu_item_id NUMBER(38,0),
    menu_item_name VARCHAR(16777216),
    item_category VARCHAR(16777216),
    item_subcategory VARCHAR(16777216),
    cost_of_goods_usd NUMBER(38,4),
    sale_price_usd NUMBER(38,4),
    menu_item_health_metrics_obj VARIANT
);

-- truck table build 
CREATE OR REPLACE TABLE <%ctx.env.SNOWFLAKE_DATABASE%>.raw_pos.truck
(
    truck_id NUMBER(38,0),
    menu_type_id NUMBER(38,0),
    primary_city VARCHAR(16777216),
    region VARCHAR(16777216),
    iso_region VARCHAR(16777216),
    country VARCHAR(16777216),
    iso_country_code VARCHAR(16777216),
    franchise_flag NUMBER(38,0),
    year NUMBER(38,0),
    make VARCHAR(16777216),
    model VARCHAR(16777216),
    ev_flag NUMBER(38,0),
    franchise_id NUMBER(38,0),
    truck_opening_date DATE
);

-- order_header table build
CREATE OR REPLACE TABLE <%ctx.env.SNOWFLAKE_DATABASE%>.raw_pos.order_header
(
    order_id NUMBER(38,0),
    truck_id NUMBER(38,0),
    location_id FLOAT,
    customer_id NUMBER(38,0),
    discount_id VARCHAR(16777216),
    shift_id NUMBER(38,0),
    shift_start_time TIME(9),
    shift_end_time TIME(9),
    order_channel VARCHAR(16777216),
    order_ts TIMESTAMP_NTZ(9),
    served_ts VARCHAR(16777216),
    order_currency VARCHAR(3),
    order_amount NUMBER(38,4),
    order_tax_amount VARCHAR(16777216),
    order_discount_amount VARCHAR(16777216),
    order_total NUMBER(38,4)
);

-- truck_reviews table build
CREATE OR REPLACE TABLE <%ctx.env.SNOWFLAKE_DATABASE%>.raw_support.truck_reviews
(
    order_id NUMBER(38,0),
    language VARCHAR(16777216),
    source VARCHAR(16777216),
    review VARCHAR(16777216),
    review_id NUMBER(18,0)
);

/*--
• harmonized view creation
--*/

-- truck_reviews_v view
CREATE OR REPLACE VIEW <%ctx.env.SNOWFLAKE_DATABASE%>.harmonized.truck_reviews_v
    AS
SELECT DISTINCT
    r.review_id,
    r.order_id,
    oh.truck_id,
    r.language,
    source,
    r.review,
    t.primary_city,
    oh.customer_id,
    TO_DATE(oh.order_ts) AS date,
    m.truck_brand_name
FROM <%ctx.env.SNOWFLAKE_DATABASE%>.raw_support.truck_reviews r
JOIN <%ctx.env.SNOWFLAKE_DATABASE%>.raw_pos.order_header oh
    ON oh.order_id = r.order_id
JOIN <%ctx.env.SNOWFLAKE_DATABASE%>.raw_pos.truck t
    ON t.truck_id = oh.truck_id
JOIN <%ctx.env.SNOWFLAKE_DATABASE%>.raw_pos.menu m
    ON m.menu_type_id = t.menu_type_id;

/*--
• analytics view creation
--*/

-- truck_reviews_v view
CREATE OR REPLACE VIEW <%ctx.env.SNOWFLAKE_DATABASE%>.analytics.truck_reviews_v
    AS
SELECT * FROM harmonized.truck_reviews_v;


/*--
raw zone table load 
--*/


-- menu table load
COPY INTO <%ctx.env.SNOWFLAKE_DATABASE%>.raw_pos.menu
FROM @<%ctx.env.SNOWFLAKE_DATABASE%>.public.s3load/raw_pos/menu/;

-- truck table load
COPY INTO <%ctx.env.SNOWFLAKE_DATABASE%>.raw_pos.truck
FROM @<%ctx.env.SNOWFLAKE_DATABASE%>.public.s3load/raw_pos/truck/;

-- order_header table load
COPY INTO <%ctx.env.SNOWFLAKE_DATABASE%>.raw_pos.order_header
FROM @<%ctx.env.SNOWFLAKE_DATABASE%>.public.s3load/raw_pos/order_header/;

-- truck_reviews table load
COPY INTO <%ctx.env.SNOWFLAKE_DATABASE%>.raw_support.truck_reviews
FROM @<%ctx.env.SNOWFLAKE_DATABASE%>.public.s3load/raw_support/truck_reviews/;


-- scale wh to medium
ALTER WAREHOUSE tasty_ds_wh SET WAREHOUSE_SIZE = 'Medium';

-- setup completion note
SELECT 'setup is now complete' AS note;