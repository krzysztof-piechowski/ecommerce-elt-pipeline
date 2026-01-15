
USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS ECOMMERCE;

CREATE SCHEMA IF NOT EXISTS ECOMMERCE.COMMON DATA_RETENTION_TIME_IN_DAYS = 7;
CREATE SCHEMA IF NOT EXISTS ECOMMERCE.BRONZE DATA_RETENTION_TIME_IN_DAYS = 14;
CREATE SCHEMA IF NOT EXISTS ECOMMERCE.SILVER DATA_RETENTION_TIME_IN_DAYS = 14;
CREATE SCHEMA IF NOT EXISTS ECOMMERCE.GOLD DATA_RETENTION_TIME_IN_DAYS = 7;


-- CREATE DEDICATED ETL WAREHOUSE
CREATE WAREHOUSE IF NOT EXISTS ETL_WH
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Dedicated warehouse for ADF ETL workloads';


--###########################################################################################
-- CLOUD INTEGRATION
--###########################################################################################

USE ROLE ACCOUNTADMIN;

CREATE STORAGE INTEGRATION IF NOT EXISTS azure_adls_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = '__AZURE_TENANT_ID__'
  STORAGE_ALLOWED_LOCATIONS = ('__STORAGE_URL__');

--DESC STORAGE INTEGRATION azure_adls_integration;

GRANT USAGE ON INTEGRATION azure_adls_integration TO ROLE SYSADMIN;
USE ROLE SYSADMIN;



--###########################################################################################
-- COMMON SCHEMA
--###########################################################################################

CREATE TABLE IF NOT EXISTS ECOMMERCE.COMMON.ETL_LOGS (
    LOG_ID              NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    LOG_UUID            STRING DEFAULT UUID_STRING(),
    PIPELINE_RUN_ID     STRING      COMMENT 'ADF Pipeline Run ID',
    
    LAYER               STRING      COMMENT 'Data Layer (Bronze/Silver/Gold)',
    PROCESS_NAME        STRING,
    TARGET_TABLE        STRING,
    STEP_NAME           STRING,
    
    STATUS              STRING      COMMENT 'START, SUCCESS, or FAIL',
    MESSAGE             STRING,
    ERROR_CODE          STRING,
    ERROR_STACK         STRING,
    
    ROWS_AFFECTED       NUMBER,
    ERROR_COUNT         NUMBER,
    DURATION_SEC        NUMBER(12,3)   COMMENT 'Execution time of the step in seconds',
    
    SNOWFLAKE_QUERY_ID  STRING,
    SESSION_ID          STRING,
    WAREHOUSE_NAME      STRING,
    USER_NAME           STRING,
    ROLE_NAME           STRING,
    
    ADDITIONAL_INFO     VARIANT     COMMENT 'JSON with technical debug details',
    
    CREATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_etl_logs PRIMARY KEY (LOG_ID)
)
--CLUSTER BY (CREATED_AT, PIPELINE_RUN_ID)
COMMENT = 'Centralized ETL logs. 1-day retention for cost savings.';



CREATE TABLE IF NOT EXISTS ECOMMERCE.COMMON.DQ_ERRORS (
    ERROR_ID            NUMBER AUTOINCREMENT,
    RUN_ID              STRING,
    BATCH_TIMESTAMP     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    LAYER               STRING,
    SOURCE_OBJECT       STRING,
    TARGET_TABLE        STRING,
    
    RECORD_KEY          STRING       COMMENT 'Primary Key value (e.g. "12345")',
    RAW_RECORD          VARIANT      COMMENT 'JSON of the bad record',
    
    ERROR_TYPE          STRING       COMMENT 'e.g. ''NEGATIVE_VALUE'', ''NULL_KEY''',
    ERROR_MESSAGE       STRING       COMMENT 'e.g. ''Validation failed: order_total_amount < 0''',
    
    CONSTRAINT PK_dq_errors PRIMARY KEY (ERROR_ID)
)
COMMENT = 'Quarantine table for rows failing Data Quality checks.';



CREATE TABLE IF NOT EXISTS ECOMMERCE.COMMON.BRONZE_INGESTION_CONFIG (
    CONFIG_ID       NUMBER AUTOINCREMENT,
    TABLE_NAME      STRING      COMMENT 'Target table in Bronze',
    ADLS_PATH       STRING      COMMENT 'Source path in ADLS',
    IS_ACTIVE       BOOLEAN DEFAULT TRUE    COMMENT 'Enable/Disable Flag',
    CREATED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Config table for dynamic Bronze ingestion loop.';

INSERT INTO ECOMMERCE.COMMON.BRONZE_INGESTION_CONFIG (TABLE_NAME, ADLS_PATH) 
VALUES 
('orders', 'orders_raw/'),
('order_items', 'order_items_raw/'),
('order_status_history', 'order_status_history_raw/'),
('payments', 'payments_raw/'),
('shipments', 'shipments_raw/'),
('products', 'products_raw/'),
('customers', 'customers_raw/'),
('addresses', 'addresses_raw/');



--###########################################################################################
-- BRONZE SCHEMA
--###########################################################################################

CREATE FILE FORMAT IF NOT EXISTS ECOMMERCE.BRONZE.fileformat_json
    TYPE = JSON
    STRIP_OUTER_ARRAY = TRUE;

CREATE STAGE IF NOT EXISTS ECOMMERCE.BRONZE.adls_stage
    URL = '__STORAGE_URL__data/'
    STORAGE_INTEGRATION = azure_adls_integration
    FILE_FORMAT = ECOMMERCE.BRONZE.fileformat_json;

--list @ECOMMERCE.BRONZE.adls_stage;


-- BRONZE ORDERS
CREATE TABLE IF NOT EXISTS ECOMMERCE.BRONZE.orders (
    payload         VARIANT     COMMENT 'Raw JSON data',
    ingestion_ts    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    source_file     STRING      COMMENT 'Audit: source filename'
)
COMMENT = 'Raw landing table for Orders (JSON).';

CREATE STREAM IF NOT EXISTS ECOMMERCE.BRONZE.orders_stream
ON TABLE ECOMMERCE.BRONZE.orders
APPEND_ONLY = TRUE
COMMENT = 'CDC stream for new inserts.';


-- BRONZE ORDER_ITEMS
CREATE TABLE IF NOT EXISTS ECOMMERCE.BRONZE.order_items (
    payload         VARIANT     COMMENT 'Raw JSON data',
    ingestion_ts    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    source_file     STRING      COMMENT 'Audit: source filename'
)
COMMENT = 'Raw landing table for Order Items (JSON).';

CREATE STREAM IF NOT EXISTS ECOMMERCE.BRONZE.order_items_stream
ON TABLE ECOMMERCE.BRONZE.order_items
APPEND_ONLY = TRUE
COMMENT = 'CDC stream for new inserts.';


-- BRONZE ORDER_STATUS_HISTORY
CREATE TABLE IF NOT EXISTS ECOMMERCE.BRONZE.order_status_history (
    payload         VARIANT     COMMENT 'Raw JSON data',
    ingestion_ts    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    source_file     STRING      COMMENT 'Audit: source filename'
)
COMMENT = 'Raw landing table for Status History (JSON).';

CREATE STREAM IF NOT EXISTS ECOMMERCE.BRONZE.order_status_history_stream
ON TABLE ECOMMERCE.BRONZE.order_status_history
APPEND_ONLY = TRUE
COMMENT = 'CDC stream for new inserts.';


-- BRONZE PAYMENTS
CREATE TABLE IF NOT EXISTS ECOMMERCE.BRONZE.payments (
    payload         VARIANT     COMMENT 'Raw JSON data',
    ingestion_ts    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    source_file     STRING      COMMENT 'Audit: source filename'
)
COMMENT = 'Raw landing table for Payments (JSON).';

CREATE STREAM IF NOT EXISTS ECOMMERCE.BRONZE.payments_stream
ON TABLE ECOMMERCE.BRONZE.payments
APPEND_ONLY = TRUE
COMMENT = 'CDC stream for new inserts.';


-- BRONZE SHIPMENTS
CREATE TABLE IF NOT EXISTS ECOMMERCE.BRONZE.shipments (
    payload         VARIANT     COMMENT 'Raw JSON data',
    ingestion_ts    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    source_file     STRING      COMMENT 'Audit: source filename'
)
COMMENT = 'Raw landing table for Shipments (JSON).';

CREATE STREAM IF NOT EXISTS ECOMMERCE.BRONZE.shipments_stream
ON TABLE ECOMMERCE.BRONZE.shipments
APPEND_ONLY = TRUE
COMMENT = 'CDC stream for new inserts.';


-- BRONZE PRODUCTS
CREATE TABLE IF NOT EXISTS ECOMMERCE.BRONZE.products (
    payload         VARIANT     COMMENT 'Raw JSON data',
    ingestion_ts    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    source_file     STRING      COMMENT 'Audit: source filename'
)
COMMENT = 'Raw landing table for Products (JSON).';

CREATE STREAM IF NOT EXISTS ECOMMERCE.BRONZE.products_stream
ON TABLE ECOMMERCE.BRONZE.products
APPEND_ONLY = TRUE
COMMENT = 'CDC stream for new inserts.';


-- BRONZE CUSTOMERS
CREATE TABLE IF NOT EXISTS ECOMMERCE.BRONZE.customers (
    payload         VARIANT     COMMENT 'Raw JSON data',
    ingestion_ts    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    source_file     STRING      COMMENT 'Audit: source filename'
)
COMMENT = 'Raw landing table for Customers (JSON).';

CREATE STREAM IF NOT EXISTS ECOMMERCE.BRONZE.customers_stream
ON TABLE ECOMMERCE.BRONZE.customers
APPEND_ONLY = TRUE
COMMENT = 'CDC stream for new inserts.';


-- BRONZE ADDRESSES
CREATE TABLE IF NOT EXISTS ECOMMERCE.BRONZE.addresses (
    payload         VARIANT     COMMENT 'Raw JSON data',
    ingestion_ts    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    source_file     STRING      COMMENT 'Audit: source filename'
)
COMMENT = 'Raw landing table for Addresses (JSON).';

CREATE STREAM IF NOT EXISTS ECOMMERCE.BRONZE.addresses_stream
ON TABLE ECOMMERCE.BRONZE.addresses
APPEND_ONLY = TRUE
COMMENT = 'CDC stream for new inserts.';




--###########################################################################################
-- SILVER SCHEMA
--###########################################################################################


-- SILVER ORDERS
CREATE TABLE IF NOT EXISTS ECOMMERCE.SILVER.orders (
    order_id            NUMBER          COMMENT 'Natural Key',
    customer_id         NUMBER,
    order_status        STRING,
    order_total_amount  NUMBER(18,2),
    currency            STRING,
    created_at          TIMESTAMP_NTZ,
    create_date         DATE,
    updated_at          TIMESTAMP_NTZ   COMMENT 'Source system timestamp',
    hash_value          STRING          COMMENT 'Hash for change detection',

    ingestion_ts        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    source_file         STRING          COMMENT 'Audit: source filename',
    CONSTRAINT PK_silver_orders PRIMARY KEY (order_id)
)
--CLUSTER BY (order_id)
COMMENT = 'Cleaned and deduplicated orders (latest state).';

CREATE STREAM IF NOT EXISTS ECOMMERCE.SILVER.orders_stream
ON TABLE ECOMMERCE.SILVER.orders;


-- SILVER ORDER_ITEMS
CREATE TABLE IF NOT EXISTS ECOMMERCE.SILVER.order_items (
    order_item_id   NUMBER              COMMENT 'Natural Key',
    order_id        NUMBER,
    product_id      NUMBER,
    quantity        NUMBER,
    unit_price      NUMBER(18,2),
    created_at      TIMESTAMP_NTZ,
    create_date     DATE,
    hash_value      STRING              COMMENT 'Hash for change detection',

    ingestion_ts    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    source_file     STRING              COMMENT 'Audit: source filename',
    CONSTRAINT PK_silver_order_items PRIMARY KEY (order_item_id)
)
--CLUSTER BY (order_id, order_item_id)
COMMENT = 'Cleaned order line items.';

CREATE STREAM IF NOT EXISTS ECOMMERCE.SILVER.order_items_stream
ON TABLE ECOMMERCE.SILVER.order_items;


-- SILVER ORDER_STATUS_HISTORY
CREATE TABLE IF NOT EXISTS ECOMMERCE.SILVER.order_status_history (
    status_history_id   NUMBER          COMMENT 'Natural Key',
    order_id            NUMBER,
    status              STRING,
    changed_at          TIMESTAMP_NTZ,

    ingestion_ts        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    source_file         STRING          COMMENT 'Audit: source filename',
    CONSTRAINT PK_silver_order_status_history PRIMARY KEY (status_history_id)
)
--CLUSTER BY (ingestion_ts)
COMMENT = 'Immutable log of order status changes (Append-only).';

CREATE STREAM IF NOT EXISTS ECOMMERCE.SILVER.order_status_history_stream
ON TABLE ECOMMERCE.SILVER.order_status_history
APPEND_ONLY = TRUE;


-- SILVER PAYMENTS
CREATE TABLE IF NOT EXISTS ECOMMERCE.SILVER.payments (
    payment_id     NUMBER           COMMENT 'Natural Key',
    order_id       NUMBER,
    provider       STRING,
    payment_status STRING,
    amount         NUMBER(18,2),
    created_at     TIMESTAMP_NTZ,
    create_date    DATE,
    updated_at     TIMESTAMP_NTZ,
    hash_value     STRING           COMMENT 'Hash for change detection',

    ingestion_ts   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    source_file    STRING           COMMENT 'Audit: source filename',
    CONSTRAINT PK_silver_payments PRIMARY KEY (payment_id)
)
--CLUSTER BY (payment_id)
COMMENT = 'Cleaned and deduplicated payments.';

CREATE STREAM IF NOT EXISTS ECOMMERCE.SILVER.payments_stream
ON TABLE ECOMMERCE.SILVER.payments;


-- SILVER SHIPMENTS
CREATE TABLE IF NOT EXISTS ECOMMERCE.SILVER.shipments (
    shipment_id     NUMBER          COMMENT 'Natural Key',
    order_id        NUMBER,
    carrier         STRING,
    shipment_status STRING,
    shipped_at      TIMESTAMP_NTZ,
    ship_date       DATE,
    delivered_at    TIMESTAMP_NTZ,
    delivery_date   DATE,
    updated_at      TIMESTAMP_NTZ,
    hash_value      STRING          COMMENT 'Hash for change detection',

    ingestion_ts    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    source_file     STRING          COMMENT 'Audit: source filename',
    CONSTRAINT PK_silver_shipments PRIMARY KEY (shipment_id)
)
--CLUSTER BY (shipment_id)
COMMENT = 'Cleaned and deduplicated shipments logic.';

CREATE STREAM IF NOT EXISTS ECOMMERCE.SILVER.shipments_stream
ON TABLE ECOMMERCE.SILVER.shipments;


-- SILVER PRODUCTS
CREATE TABLE IF NOT EXISTS ECOMMERCE.SILVER.products (
    product_id   NUMBER         COMMENT 'Natural Key',
    name         STRING,
    category     STRING,
    brand        STRING,
    price        NUMBER(18,2),
    currency     STRING,
    status       STRING,
    created_at   TIMESTAMP_NTZ,
    create_date  DATE,
    updated_at   TIMESTAMP_NTZ,
    hash_value   STRING         COMMENT 'Hash for change detection',

    ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    source_file  STRING         COMMENT 'Audit: source filename',
    CONSTRAINT PK_silver_products PRIMARY KEY (product_id)
)
COMMENT = 'Cleaned product catalog (latest state).';

CREATE STREAM IF NOT EXISTS ECOMMERCE.SILVER.products_stream
ON TABLE ECOMMERCE.SILVER.products;


-- SILVER CUSTOMERS
CREATE TABLE IF NOT EXISTS ECOMMERCE.SILVER.customers (
    customer_id  NUMBER         COMMENT 'Natural Key',
    email        STRING         COMMENT 'PII data',
    first_name   STRING,
    last_name    STRING,
    phone        STRING         COMMENT 'PII data',
    status       STRING,
    created_at   TIMESTAMP_NTZ,
    create_date  DATE,
    updated_at   TIMESTAMP_NTZ,
    hash_value   STRING         COMMENT 'Hash for change detection',

    ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    source_file  STRING         COMMENT 'Audit: source filename',
    CONSTRAINT PK_silver_customers PRIMARY KEY (customer_id)
)
--CLUSTER BY (customer_id)
COMMENT = 'Cleaned customer profiles. Contains PII.';

CREATE STREAM IF NOT EXISTS ECOMMERCE.SILVER.customers_stream
ON TABLE ECOMMERCE.SILVER.customers;


-- SILVER ADDRESSES
CREATE TABLE IF NOT EXISTS ECOMMERCE.SILVER.addresses (
    address_id   NUMBER         COMMENT 'Natural Key',
    customer_id  NUMBER,
    type         STRING,
    street       STRING,
    city         STRING,
    postal_code  STRING,
    country      STRING,
    is_default   BOOLEAN,
    created_at   TIMESTAMP_NTZ,
    create_date  DATE,
    updated_at   TIMESTAMP_NTZ,
    hash_value   STRING         COMMENT 'Hash for change detection',

    ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    source_file  STRING         COMMENT 'Audit: source filename',
    CONSTRAINT PK_silver_addresses PRIMARY KEY (address_id)
)
--CLUSTER BY (customer_id, address_id)
COMMENT = 'Cleaned address data.';

CREATE STREAM IF NOT EXISTS ECOMMERCE.SILVER.addresses_stream
ON TABLE ECOMMERCE.SILVER.addresses;


--###########################################################################################
-- GOLD SCHEMA
--###########################################################################################


-- CREATE STATIC DIM_DATE TABLE
CREATE TABLE IF NOT EXISTS ECOMMERCE.GOLD.dim_date (
    date_key            NUMBER      COMMENT 'PK: YYYYMMDD format',
    date_value          DATE,
    day_num             NUMBER,
    day_name            VARCHAR(15),
    day_name_short      VARCHAR(3),
    day_of_week         NUMBER,
    day_of_year         NUMBER,
    week_of_year        NUMBER,
    month_num           NUMBER,
    month_name          VARCHAR(15),
    month_name_short    VARCHAR(3),
    quarter             NUMBER,
    year                NUMBER,
    year_month          VARCHAR(7),
    is_weekend          BOOLEAN     COMMENT 'True if Sat/Sun',
    is_last_day_of_month BOOLEAN,
    epoch_days          NUMBER,
    CONSTRAINT PK_dim_date PRIMARY KEY (date_key)
)
COMMENT = 'Static Date Dimension. Used for calendar-based analysis and drill-downs.';

INSERT INTO ECOMMERCE.GOLD.dim_date
SELECT
    TO_NUMBER(TO_CHAR(d, 'YYYYMMDD')) AS date_key,
    d AS date_value,
    DAY(d) AS day_num,
    DECODE(DAYOFWEEKISO(d), 
        1, 'Monday', 2, 'Tuesday', 3, 'Wednesday', 4, 'Thursday', 
        5, 'Friday', 6, 'Saturday', 7, 'Sunday') AS day_name,
    DAYNAME(d) AS day_name_short,
    DAYOFWEEKISO(d) AS day_of_week,
    DAYOFYEAR(d) AS day_of_year,
    WEEKISO(d) AS week_of_year,
    MONTH(d) AS month_num,
    DECODE(MONTH(d),
        1, 'January', 2, 'February', 3, 'March', 4, 'April', 5, 'May', 6, 'June',
        7, 'July', 8, 'August', 9, 'September', 10, 'October', 11, 'November', 12, 'December') AS month_name,
    MONTHNAME(d) AS month_name_short,
    QUARTER(d) AS quarter,
    YEAR(d) AS year,
    TO_CHAR(d, 'YYYY-MM') AS year_month,
    IFF(DAYOFWEEKISO(d) IN (6, 7), TRUE, FALSE) AS is_weekend,
    IFF(d = LAST_DAY(d), TRUE, FALSE) AS is_last_day_of_month,
    DATEDIFF(day, '1970-01-01', d) AS epoch_days
FROM (
    SELECT DATEADD(day, ROW_NUMBER() OVER (ORDER BY NULL) - 1, '2000-01-01') AS d
    FROM TABLE(GENERATOR(ROWCOUNT => 20000)) -- ~55 years of dates
);


-- CREATE DIM_PRODUCTS
CREATE TABLE IF NOT EXISTS ECOMMERCE.GOLD.dim_products (
    product_key     NUMBER AUTOINCREMENT    COMMENT 'Surrogate Key (PK)',
    product_id      NUMBER                  COMMENT 'Natural Key',
    
    name            STRING,
    category        STRING,
    brand           STRING,
    price           NUMBER(18,2),
    currency        STRING,
    status          STRING,
    hash_value      STRING,
    
    valid_from      TIMESTAMP_NTZ       COMMENT 'SCD2: Start of validity',
    valid_to        TIMESTAMP_NTZ       COMMENT 'SCD2: End of validity',
    is_current      BOOLEAN             COMMENT 'SCD2: True if latest version',
    CONSTRAINT PK_dim_products PRIMARY KEY (product_key)
) --CLUSTER BY (product_id)
COMMENT = 'SCD Type 2 Dimension for Products. Tracks price and status history.';


-- CREATE TECHNICAL RECORD FOR MISSING VALUES
MERGE INTO ECOMMERCE.GOLD.dim_products d
USING (SELECT -1 AS product_key) s
ON d.product_key = s.product_key
WHEN NOT MATCHED THEN
    INSERT (
        product_key, product_id, name, category, brand, price, currency, 
        status, hash_value, valid_from, valid_to, is_current
    )
    VALUES (
        -1, -1, 'Unknown Product', 'Unknown', 'Unknown', 0, 'EUR', 
        'Unknown', '0', '1900-01-01', '9999-12-31', TRUE
);


-- CREATE DIM_CUSTOMERS
CREATE TABLE IF NOT EXISTS ECOMMERCE.GOLD.dim_customers (
    customer_key    NUMBER AUTOINCREMENT    COMMENT 'Surrogate Key (PK)',
    customer_id     NUMBER                  COMMENT 'Natural Key',
    
    email           STRING              COMMENT 'Masked PII',
    first_name      STRING,
    last_name       STRING,
    phone           STRING              COMMENT 'Masked PII',
    
    city            STRING,
    country         STRING,
    postal_code     STRING,
    street          STRING,
    address_type    STRING,
    
    status          STRING,
    hash_value      STRING,

    valid_from      TIMESTAMP_NTZ       COMMENT 'SCD2: Start of validity',
    valid_to        TIMESTAMP_NTZ       COMMENT 'SCD2: End of validity',
    is_current      BOOLEAN             COMMENT 'SCD2: True if latest version',
    CONSTRAINT PK_dim_customers PRIMARY KEY (customer_key)
)
--CLUSTER BY (customer_id)
COMMENT = 'SCD Type 2 Dimension for Customers. Contains masked PII data.';


-- CREATE TECHNICAL RECORD FOR MISSING VALUES
MERGE INTO ECOMMERCE.GOLD.dim_customers d
USING (SELECT -1 AS customer_key) s
ON d.customer_key = s.customer_key
WHEN NOT MATCHED THEN
    INSERT (
        customer_key, customer_id, email, first_name, last_name, phone,
        city, country, postal_code, street, address_type, status, hash_value, valid_from, valid_to, is_current
    )
    VALUES (
        -1, -1, 'Unknown', 'Unknown', 'Unknown', 'Unknown',
        'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'ACTIVE', '0', '1900-01-01', '9999-12-31', TRUE
);


-- CREATE FACT_ORDERS
CREATE TABLE IF NOT EXISTS ECOMMERCE.GOLD.fact_orders (
    fact_order_key      NUMBER AUTOINCREMENT,
    order_id            NUMBER              COMMENT 'Natural Key',
    
    customer_key        NUMBER              COMMENT 'FK to DIM_CUSTOMERS',
    order_date_key      NUMBER              COMMENT 'FK to DIM_DATE',
    
    order_status        STRING,
    order_total_amount  NUMBER(18,2)        COMMENT 'Metric: Total value',
    currency            STRING,
    
    created_at          TIMESTAMP_NTZ,
    updated_at          TIMESTAMP_NTZ,
    ingestion_ts        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT PK_fact_orders PRIMARY KEY (fact_order_key)
) --CLUSTER BY (order_date_key)
COMMENT = 'Transactional Fact Table. Grain: One row per Order Header.';


-- CREATE FACT_SALES
CREATE TABLE IF NOT EXISTS ECOMMERCE.GOLD.fact_sales (
    sales_key           NUMBER AUTOINCREMENT,
    order_item_id       NUMBER          COMMENT 'Natural Key / Degenerate Dim',
    order_id            NUMBER,
    
    product_key         NUMBER          COMMENT 'FK to DIM_PRODUCTS',
    customer_key        NUMBER          COMMENT 'FK to DIM_CUSTOMERS',
    order_date_key      NUMBER          COMMENT 'FK to DIM_DATE',
    
    quantity            NUMBER          COMMENT 'Metric: Units sold',
    unit_price          NUMBER(18,2),
    total_line_amount   NUMBER(18,2)    COMMENT 'Metric: Revenue (Qty * Price)',
    currency            STRING,
    
    ingestion_ts        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT PK_fact_sales PRIMARY KEY (sales_key)
) --CLUSTER BY (order_date_key)
COMMENT = 'Transactional Fact Table. Grain: One row per Order Line Item.';



-- CREATE FACT_SHIPMENTS
CREATE TABLE IF NOT EXISTS ECOMMERCE.GOLD.fact_shipments (
    shipment_key        NUMBER AUTOINCREMENT,
    shipment_id         NUMBER          COMMENT 'Natural Key',
    order_id            NUMBER,
    
    customer_key        NUMBER          COMMENT 'FK to DIM_CUSTOMERS',
    ship_date_key       NUMBER          COMMENT 'FK to DIM_DATE',
    delivery_date_key   NUMBER          COMMENT 'FK to DIM_DATE',
    shipped_at          TIMESTAMP_NTZ,
    delivered_at        TIMESTAMP_NTZ,
    
    carrier             STRING,
    shipment_status     STRING,
    
    days_to_ship        NUMBER          COMMENT 'KPI: Created -> Shipped',
    days_to_deliver     NUMBER          COMMENT 'KPI: Shipped -> Delivered',
    
    ingestion_ts        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT PK_fact_shipments PRIMARY KEY (shipment_key)
) --CLUSTER BY (ship_date_key)
COMMENT = 'Accumulating Snapshot Fact. Tracks shipping performance and lifecycle.';


-- CREATE FACT_PAYMENTS
CREATE TABLE IF NOT EXISTS ECOMMERCE.GOLD.fact_payments (
    payment_key         NUMBER AUTOINCREMENT,
    payment_id          NUMBER          COMMENT 'Natural Key',
    order_id            NUMBER,
    
    customer_key        NUMBER          COMMENT 'FK to DIM_CUSTOMERS',
    date_key            NUMBER          COMMENT 'FK to DIM_DATE',
    
    provider            STRING,
    payment_status      STRING,
    
    amount              NUMBER(18,2)    COMMENT 'Metric: Payment value',
    
    ingestion_ts        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT PK_fact_payments PRIMARY KEY (payment_key)
) --CLUSTER BY (date_key)
COMMENT = 'Transactional Fact Table for Payments.';


-- CREATE FACT_ORDER_STATUS_HISTORY
CREATE TABLE IF NOT EXISTS ECOMMERCE.GOLD.fact_order_status_history (
    history_key         NUMBER AUTOINCREMENT,
    status_history_id   NUMBER,
    order_id            NUMBER,
    
    date_key            NUMBER          COMMENT 'FK to DIM_DATE',
    changed_at_ts       TIMESTAMP_NTZ, 
    
    status              STRING          COMMENT 'New Status',
    
    ingestion_ts        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT PK_fact_order_status_history PRIMARY KEY (history_key)
) --CLUSTER BY (date_key)
COMMENT = 'Factless Fact Table (Events). Logs every status change event.';





CREATE OR REPLACE VIEW ECOMMERCE.GOLD.V_DIM_CUSTOMERS 
COMMENT = 'Reporting View for Customers (SCD2).'
AS
SELECT * FROM ECOMMERCE.GOLD.DIM_CUSTOMERS;

CREATE OR REPLACE VIEW ECOMMERCE.GOLD.V_DIM_PRODUCTS
COMMENT = 'Reporting View for Products (SCD2).'
AS
SELECT * FROM ECOMMERCE.GOLD.DIM_PRODUCTS;

CREATE OR REPLACE VIEW ECOMMERCE.GOLD.V_FACT_ORDERS
COMMENT = 'Reporting View for Orders.'
AS
SELECT * FROM ECOMMERCE.GOLD.FACT_ORDERS;

CREATE OR REPLACE VIEW ECOMMERCE.GOLD.V_FACT_SALES
COMMENT = 'Reporting View for Sales.'
AS
SELECT * FROM ECOMMERCE.GOLD.FACT_SALES;

CREATE OR REPLACE VIEW ECOMMERCE.GOLD.V_FACT_PAYMENTS
COMMENT = 'Reporting View for Payments.'
AS
SELECT * FROM ECOMMERCE.GOLD.FACT_PAYMENTS;

CREATE OR REPLACE VIEW ECOMMERCE.GOLD.V_FACT_SHIPMENTS
COMMENT = 'Reporting View for Shipments.'
AS
SELECT * FROM ECOMMERCE.GOLD.FACT_SHIPMENTS;

CREATE OR REPLACE VIEW ECOMMERCE.GOLD.V_FACT_ORDER_STATUS_HISTORY
COMMENT = 'Reporting View for Status History.'
AS 
SELECT * FROM ECOMMERCE.GOLD.FACT_ORDER_STATUS_HISTORY;



--###########################################################################################
-- CREATE ETL_ROLE
--###########################################################################################

USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS ETL_ROLE;

CREATE USER IF NOT EXISTS ETL_USER
    PASSWORD = '__ETL_PASSWORD__'
    DEFAULT_ROLE = ETL_ROLE
    DEFAULT_WAREHOUSE = compute_wh
    MUST_CHANGE_PASSWORD = FALSE;

GRANT ROLE ETL_ROLE TO USER ETL_USER;


USE ROLE SYSADMIN;
GRANT USAGE ON WAREHOUSE ETL_WH TO ROLE ETL_ROLE;

GRANT USAGE ON DATABASE ECOMMERCE TO ROLE ETL_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE ECOMMERCE TO ROLE ETL_ROLE;

GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA ECOMMERCE.COMMON TO ROLE ETL_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA ECOMMERCE.BRONZE TO ROLE ETL_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA ECOMMERCE.SILVER TO ROLE ETL_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA ECOMMERCE.GOLD TO ROLE ETL_ROLE;

GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA ECOMMERCE.COMMON TO ROLE ETL_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA ECOMMERCE.BRONZE TO ROLE ETL_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA ECOMMERCE.SILVER TO ROLE ETL_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA ECOMMERCE.GOLD TO ROLE ETL_ROLE;

GRANT USAGE ON ALL PROCEDURES IN SCHEMA ECOMMERCE.COMMON TO ROLE ETL_ROLE;
GRANT USAGE ON ALL PROCEDURES IN SCHEMA ECOMMERCE.BRONZE TO ROLE ETL_ROLE;
GRANT USAGE ON ALL PROCEDURES IN SCHEMA ECOMMERCE.SILVER TO ROLE ETL_ROLE;
GRANT USAGE ON ALL PROCEDURES IN SCHEMA ECOMMERCE.GOLD TO ROLE ETL_ROLE;

GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA ECOMMERCE.COMMON TO ROLE ETL_ROLE;
GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA ECOMMERCE.BRONZE TO ROLE ETL_ROLE;
GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA ECOMMERCE.SILVER TO ROLE ETL_ROLE;
GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA ECOMMERCE.GOLD TO ROLE ETL_ROLE;

GRANT USAGE, READ ON ALL STAGES IN SCHEMA ECOMMERCE.BRONZE TO ROLE ETL_ROLE;

GRANT USAGE ON ALL FILE FORMATS IN SCHEMA ECOMMERCE.BRONZE TO ROLE ETL_ROLE;

GRANT SELECT ON ALL STREAMS IN SCHEMA ECOMMERCE.BRONZE TO ROLE ETL_ROLE;
GRANT SELECT ON ALL STREAMS IN SCHEMA ECOMMERCE.SILVER TO ROLE ETL_ROLE;



--###########################################################################################
-- CREATE REPORTER_ROLE
--###########################################################################################

USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS REPORTER_ROLE;

CREATE USER IF NOT EXISTS BI_USER 
    PASSWORD='__BI_PASSWORD__' 
    DEFAULT_ROLE=REPORTER_ROLE;
    
GRANT ROLE REPORTER_ROLE TO USER BI_USER;

USE ROLE SYSADMIN;

GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE REPORTER_ROLE;
GRANT USAGE ON DATABASE ECOMMERCE TO ROLE REPORTER_ROLE;
GRANT USAGE ON SCHEMA ECOMMERCE.GOLD TO ROLE REPORTER_ROLE;

GRANT SELECT ON ALL TABLES IN SCHEMA ECOMMERCE.GOLD TO ROLE REPORTER_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA ECOMMERCE.GOLD TO ROLE REPORTER_ROLE;

GRANT SELECT ON FUTURE TABLES IN SCHEMA ECOMMERCE.GOLD TO ROLE REPORTER_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA ECOMMERCE.GOLD TO ROLE REPORTER_ROLE;


--###########################################################################################
-- CREATE MASKING POLICY
--###########################################################################################

USE ROLE ACCOUNTADMIN;

CREATE MASKING POLICY IF NOT EXISTS ECOMMERCE.COMMON.MASK_PII AS (val string) RETURNS string ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'ETL_ROLE') THEN val
        ELSE '***MASKED***'
    END;

GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE SYSADMIN;

USE ROLE SYSADMIN;
ALTER TABLE ECOMMERCE.SILVER.CUSTOMERS MODIFY COLUMN email SET MASKING POLICY ECOMMERCE.COMMON.MASK_PII;
ALTER TABLE ECOMMERCE.SILVER.CUSTOMERS MODIFY COLUMN phone SET MASKING POLICY ECOMMERCE.COMMON.MASK_PII;

ALTER TABLE ECOMMERCE.GOLD.DIM_CUSTOMERS MODIFY COLUMN email SET MASKING POLICY ECOMMERCE.COMMON.MASK_PII;
ALTER TABLE ECOMMERCE.GOLD.DIM_CUSTOMERS MODIFY COLUMN phone SET MASKING POLICY ECOMMERCE.COMMON.MASK_PII;