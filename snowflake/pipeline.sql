CREATE DATABASE e_com_db;
CREATE SCHEMA e_com_db.raw;
CREATE SCHEMA e_com_db.analytic;

USE e_com_db.raw;

CREATE OR REPLACE TABLE ORDER_EVENTS (
    order_event VARIANT
);

-- secure connection between Snowflake and S3
CREATE OR REPLACE STORAGE INTEGRATION e_com_pipeline_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::490909520939:role/e-com-pipeline-role'
STORAGE_ALLOWED_LOCATIONS = ('s3://e-com-pipeline/raw/');

-- For describe above integration
DESC INTEGRATION e_com_pipeline_int;

CREATE OR REPLACE FILE FORMAT orders_format
TYPE = 'JSON';

-- Create stage 
CREATE OR REPLACE STAGE orders_stage
STORAGE_INTEGRATION = e_com_pipeline_int
URL = 's3://e-com-pipeline/raw/'
FILE_FORMAT = orders_format;

list @orders_stage;

CREATE OR REPLACE PIPE E_COM_ORDERS_PIPE
AUTO_INGEST = TRUE AS
COPY into order_events(order_event)
FROM @orders_stage
FILE_FORMAT = (FORMAT_NAME = orders_format);

SELECT SYSTEM$PIPE_STATUS('E_COM_ORDERS_PIPE');

SHOW PIPES;

SELECT * FROM e_com_db.raw.order_events;

ALTER PIPE e_com_db.raw.E_COM_ORDERS_PIPE SET PIPE_EXECUTION_PAUSED = TRUE;

ALTER PIPE e_com_db.raw.E_COM_ORDERS_PIPE SET PIPE_EXECUTION_PAUSED = FALSE;


USE e_com_db.analytic;

-- Customers table
CREATE OR REPLACE TABLE customers (
    customer_id INT PRIMARY KEY,
    name STRING,
    email STRING,
    address STRING
);

-- Orders table
CREATE OR REPLACE TABLE orders (
    order_id INT PRIMARY KEY,
    order_date DATE,
    total_amount FLOAT,
    customer_id INT REFERENCES customers(customer_id)
);

-- Order Products table
CREATE OR REPLACE TABLE order_products (
    order_id INT REFERENCES orders(order_id),
    product_id STRING,
    name STRING,
    category STRING,
    price FLOAT,
    quantity INT
);

-- Create a STREAM on Raw Table
CREATE OR REPLACE STREAM e_com_db.raw.order_events_stream
ON TABLE e_com_db.raw.ORDER_EVENTS
APPEND_ONLY = TRUE;




CREATE OR REPLACE PROCEDURE e_com_db.analytic.transform_orders_sp()
RETURNS STRING
LANGUAGE SQL
AS
$$

BEGIN

-- Customers
MERGE INTO e_com_db.analytic.customers AS target
USING (
    SELECT DISTINCT
           order_event:customer.customer_id::INT AS customer_id,
           order_event:customer.name::STRING AS name,
           order_event:customer.email::STRING AS email,
           order_event:customer.address::STRING AS address
    FROM e_com_db.raw.order_events_stream
) AS source
ON target.customer_id = source.customer_id
WHEN NOT MATCHED THEN
    INSERT (customer_id, name, email, address)
    VALUES (source.customer_id, source.name, source.email, source.address);

-- Orders
MERGE INTO e_com_db.analytic.orders AS target
USING (
    SELECT
        order_event:order_id::INT AS order_id,
        order_event:order_date::DATE AS order_date,
        order_event:total_amount::FLOAT AS total_amount,
        order_event:customer.customer_id::INT AS customer_id
    FROM e_com_db.raw.order_events_stream
) AS source
ON target.order_id = source.order_id
WHEN NOT MATCHED THEN
    INSERT (order_id, order_date, total_amount, customer_id)
    VALUES (source.order_id, source.order_date, source.total_amount, source.customer_id);

-- Order Products
MERGE INTO e_com_db.analytic.order_products AS target
USING (
    SELECT
        order_event:order_id::INT AS order_id,
        p.value:product_id::STRING AS product_id,
        p.value:name::STRING AS name,
        p.value:category::STRING AS category,
        p.value:price::FLOAT AS price,
        p.value:quantity::INT AS quantity
    FROM e_com_db.raw.order_events_stream,
         LATERAL FLATTEN(input => order_event:products) p
) AS source
ON target.order_id = source.order_id
   AND target.product_id = source.product_id
WHEN NOT MATCHED THEN
    INSERT (order_id, product_id, name, category, price, quantity)
    VALUES (source.order_id, source.product_id, source.name, source.category, source.price, source.quantity);

RETURN 'SUCCESS';

END;
$$;




CREATE OR REPLACE TASK e_com_db.analytic.transform_orders_task
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
WHEN
    SYSTEM$STREAM_HAS_DATA('e_com_db.raw.order_events_stream')
AS
CALL e_com_db.analytic.transform_orders_sp();






-- CREATE OR REPLACE TASK e_com_db.analytic.transform_orders_task
-- WAREHOUSE = COMPUTE_WH
-- SCHEDULE = '1 MINUTE'
-- WHEN
--     SYSTEM$STREAM_HAS_DATA('e_com_db.raw.order_events_stream')
-- AS

-- -- Customers
-- MERGE INTO e_com_db.analytic.customers AS target
-- USING (
--     SELECT DISTINCT
--            order_event:customer.customer_id::INT AS customer_id,
--            order_event:customer.name::STRING AS name,
--            order_event:customer.email::STRING AS email,
--            order_event:customer.address::STRING AS address
--     FROM e_com_db.raw.order_events_stream
-- ) AS source
-- ON target.customer_id = source.customer_id
-- WHEN NOT MATCHED THEN
--     INSERT (customer_id, name, email, address)
--     VALUES (source.customer_id, source.name, source.email, source.address);

-- -- Orders
-- MERGE INTO e_com_db.analytic.orders AS target
-- USING (
--     SELECT
--         order_event:order_id::INT AS order_id,
--         order_event:order_date::DATE AS order_date,
--         order_event:total_amount::FLOAT AS total_amount,
--         order_event:customer.customer_id::INT AS customer_id
--     FROM e_com_db.raw.order_events_stream
-- ) AS source
-- ON target.order_id = source.order_id
-- WHEN NOT MATCHED THEN
--     INSERT (order_id, order_date, total_amount, customer_id)
--     VALUES (source.order_id, source.order_date, source.total_amount, source.customer_id);

-- -- Order Products
-- MERGE INTO e_com_db.analytic.order_products AS target
-- USING (
--     SELECT
--         order_event:order_id::INT AS order_id,
--         p.value:product_id::STRING AS product_id,
--         p.value:name::STRING AS name,
--         p.value:category::STRING AS category,
--         p.value:price::FLOAT AS price,
--         p.value:quantity::INT AS quantity
--     FROM e_com_db.raw.order_events_stream,
--          LATERAL FLATTEN(input => order_event:products) p
-- ) AS source
-- ON target.order_id = source.order_id
--    AND target.product_id = source.product_id
-- WHEN NOT MATCHED THEN
--     INSERT (order_id, product_id, name, category, price, quantity)
--     VALUES (source.order_id, source.product_id, source.name, source.category, source.price, source.quantity);


ALTER TASK e_com_db.analytic.transform_orders_task RESUME;

ALTER TASK e_com_db.analytic.transform_orders_task SUSPEND;

SHOW TASKS LIKE 'TRANSFORM_ORDERS_TASK';

SELECT SYSTEM$STREAM_HAS_DATA('e_com_db.raw.order_events_stream');

SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TRANSFORM_ORDERS_TASK',
    RESULT_LIMIT => 10
));