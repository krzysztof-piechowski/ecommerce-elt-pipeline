
USE ROLE SYSADMIN;

-- #####################################################
-- PROC LOG_EVENT (For procedure logs)
-- #####################################################

CREATE OR REPLACE PROCEDURE ECOMMERCE.COMMON.LOG_EVENT(
    P_RUN_ID STRING,
    P_LAYER STRING,
    P_PROCESS_NAME STRING,
    P_TARGET_TABLE STRING,
    P_STEP_NAME STRING,
    P_STATUS STRING,
    P_MESSAGE STRING,
    P_ROWS_AFFECTED NUMBER,
    P_ERROR_COUNT NUMBER,
    P_DURATION_SEC NUMBER(12,3),
    P_ERROR_CODE STRING,
    P_ERROR_STACK STRING,
    P_ADDITIONAL_INFO VARIANT
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    INSERT INTO ECOMMERCE.COMMON.ETL_LOGS (
        PIPELINE_RUN_ID, LAYER, PROCESS_NAME, TARGET_TABLE, STEP_NAME,
        STATUS, MESSAGE, ROWS_AFFECTED, ERROR_COUNT, DURATION_SEC,
        ERROR_CODE, ERROR_STACK, ADDITIONAL_INFO,
        SNOWFLAKE_QUERY_ID, SESSION_ID, WAREHOUSE_NAME, USER_NAME, ROLE_NAME
    )
    SELECT
        :P_RUN_ID, :P_LAYER, :P_PROCESS_NAME, :P_TARGET_TABLE, :P_STEP_NAME,
        :P_STATUS, :P_MESSAGE, :P_ROWS_AFFECTED, :P_ERROR_COUNT, :P_DURATION_SEC,
        :P_ERROR_CODE, :P_ERROR_STACK, :P_ADDITIONAL_INFO,
        LAST_QUERY_ID(),
        CURRENT_SESSION(),
        CURRENT_WAREHOUSE(),
        CURRENT_USER(),
        CURRENT_ROLE();
        
    RETURN 'LOGGED';
END;
$$;




-- ##########################################################################################################
-- BRONZE LAYER PROCEDURES
-- ##########################################################################################################

-- ###################################
-- PROC LOAD_BRONZE_MASTER (Loading files)
-- ###################################
CREATE OR REPLACE PROCEDURE ECOMMERCE.BRONZE.LOAD_BRONZE_MASTER(RUN_ID STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    c1 CURSOR FOR 
        SELECT TABLE_NAME, ADLS_PATH 
        FROM ECOMMERCE.COMMON.BRONZE_INGESTION_CONFIG 
        WHERE IS_ACTIVE = TRUE;

    current_table STRING;
    current_path STRING;
    sql_command STRING;
    
    rows_cnt NUMBER;
    step_start_ts TIMESTAMP;
    step_duration NUMBER(12,3);
    proc_start_ts TIMESTAMP;
    total_duration NUMBER(12,3);
    custom_meta VARIANT;
    loaded_files_list VARIANT;
    
BEGIN
    proc_start_ts := CURRENT_TIMESTAMP();

    BEGIN TRANSACTION;

    CALL ECOMMERCE.COMMON.LOG_EVENT(:RUN_ID, 'BRONZE', 'MASTER', NULL, 'INIT', 'START', 'Starting Bronze Loop', 0, 0, 0, NULL, NULL, NULL);
    
    OPEN c1;
    FOR record IN c1 DO
        current_table := record.TABLE_NAME;
        current_path  := record.ADLS_PATH;
        
        step_start_ts := CURRENT_TIMESTAMP();
        
        sql_command := '
            COPY INTO ECOMMERCE.BRONZE.' || :current_table || ' (payload, source_file)
            FROM (
                SELECT $1, METADATA$FILENAME 
                FROM @ECOMMERCE.BRONZE.adls_stage/' || :current_path || '
            )
            ON_ERROR = ''CONTINUE''
        ';

        EXECUTE IMMEDIATE :sql_command;
        rows_cnt := SQLROWCOUNT;

        BEGIN
            SELECT ARRAY_AGG("file") 
            INTO :loaded_files_list
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
            WHERE "status" = 'LOADED';
        EXCEPTION
            WHEN OTHER THEN
                loaded_files_list := [];
        END;

        step_duration := DATEDIFF('millisecond', :step_start_ts, CURRENT_TIMESTAMP()) / 1000;

        custom_meta := OBJECT_CONSTRUCT(
            'load_strategy',   'INGEST_COPY',
            'source_obj',      '@ECOMMERCE.BRONZE.adls_stage/' || :current_path,
            'business_key',    'filename',
            'description',     'Raw file ingestion from ADLS',
            'processed_files', IFF(:loaded_files_list IS NULL, [], :loaded_files_list)
        );

        CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'BRONZE', 'LOAD_BRONZE', UPPER(:current_table), 'COPY', 
            'SUCCESS', 'File Loaded', :rows_cnt, 0, :step_duration, NULL, NULL, :custom_meta
        );
        
    END FOR;
    CLOSE c1;

    COMMIT;
    

    -- ###################################
    -- FINISH
    -- ###################################

    total_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;
    CALL ECOMMERCE.COMMON.LOG_EVENT(
        :RUN_ID, 'BRONZE', 'MASTER', NULL, 'FINISH', 
        'SUCCESS', 'Bronze Loop Completed', 0, 0, :total_duration, NULL, NULL, NULL
    );
    
    RETURN 'BRONZE MASTER LOAD COMPLETED SUCCESSFULLY';

EXCEPTION
    WHEN OTHER THEN
    
        ROLLBACK;
        
        LET err_code STRING := SQLCODE;
        LET err_msg STRING := SQLERRM;
        LET err_state STRING := SQLSTATE;
        LET err_details VARIANT := OBJECT_CONSTRUCT(
            'failed_at_table', :current_table,
            'failed_at_path',  :current_path,
            'sql_state',       :err_state
        );
        
        total_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;        
        
        CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'BRONZE', 'MASTER', UPPER(:current_table), 'FAILURE', 
            'FAIL', :err_msg, 0, 0, :total_duration, :err_code, :err_state, :err_details
        );
        RAISE;
END;
$$;





-- ##########################################################################################################
-- SILVER LAYER PROCEDURES
-- ##########################################################################################################

-- ###################################
-- PROC LOAD_SILVER_ORDERS
-- ###################################

CREATE OR REPLACE PROCEDURE ECOMMERCE.SILVER.LOAD_ORDERS(RUN_ID STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    proc_start_ts TIMESTAMP;
    proc_duration NUMBER(12,3);
    rows_cnt NUMBER;
    error_cnt NUMBER;
    custom_meta VARIANT;

    PROC_NAME STRING := 'ORDERS';

BEGIN
    proc_start_ts := CURRENT_TIMESTAMP();    

    BEGIN TRANSACTION;

    CALL ECOMMERCE.COMMON.LOG_EVENT(
        :RUN_ID, 'SILVER', :PROC_NAME, :PROC_NAME, 'INIT',
        'START', 'Starting load', 0, 0, 0, NULL, NULL, NULL
    );
    
    -- Data Quality Checks
    INSERT INTO ECOMMERCE.COMMON.DQ_ERRORS (
        RUN_ID, LAYER, SOURCE_OBJECT, TARGET_TABLE, RECORD_KEY, RAW_RECORD, ERROR_TYPE, ERROR_MESSAGE
    )
    SELECT 
        :RUN_ID, 
        'SILVER', 
        'orders_stream', 
        'orders', 
        payload:order_id::STRING, 
        payload, 
        CASE 
            WHEN payload:order_total_amount::NUMBER(18,2) < 0 THEN 'NEGATIVE_AMOUNT'
            WHEN payload:customer_id IS NULL THEN 'MISSING_CUSTOMER'
            WHEN payload:created_at::TIMESTAMP_NTZ IS NULL THEN 'MISSING_DATE'
            ELSE 'UNKNOWN'
        END,
        'Validation Failed'
    FROM ECOMMERCE.BRONZE.orders_stream
    WHERE metadata$action = 'INSERT'
      AND ( 
          payload:order_total_amount::NUMBER(18,2) < 0 
          OR payload:customer_id IS NULL
          OR payload:created_at::TIMESTAMP_NTZ IS NULL
      );

    error_cnt := SQLROWCOUNT;


    -- Main Merge Logic
    MERGE INTO ECOMMERCE.SILVER.orders s
    USING (
        SELECT
            payload:order_id::NUMBER            AS order_id,
            payload:customer_id::NUMBER         AS customer_id,
            payload:order_status::STRING        AS order_status,
            payload:order_total_amount::NUMBER(18,2) AS order_total_amount,
            payload:currency::STRING            AS currency,
            payload:created_at::TIMESTAMP_NTZ   AS created_at,
            CAST(payload:created_at::TIMESTAMP_NTZ AS DATE) as create_date,
            payload:updated_at::TIMESTAMP_NTZ   AS updated_at,
    
            SHA2_HEX(CONCAT_WS('|',
                COALESCE(payload:order_status::STRING, ''),
                COALESCE(payload:order_total_amount::STRING, '0'),
                COALESCE(payload:currency::STRING, '')
            )) AS hash_value,
        
            source_file
        FROM ECOMMERCE.BRONZE.orders_stream
        WHERE metadata$action = 'INSERT'
          AND NOT (
              payload:order_total_amount::NUMBER(18,2) < 0 
              OR payload:customer_id IS NULL
              OR payload:created_at::TIMESTAMP_NTZ IS NULL
          )
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY payload:order_id::NUMBER 
            ORDER BY payload:updated_at::TIMESTAMP_NTZ DESC, ingestion_ts DESC
        ) = 1
    ) r
    ON s.order_id = r.order_id
    WHEN MATCHED 
        --AND r.updated_at > s.updated_at 
        AND r.hash_value != s.hash_value
    THEN 
        UPDATE SET 
        customer_id         = r.customer_id,
        order_status        = r.order_status,
        order_total_amount  = r.order_total_amount,
        currency            = r.currency,
        hash_value          = r.hash_value,
        
        updated_at          = r.updated_at,
        source_file         = r.source_file,
        ingestion_ts        = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN 
    INSERT (
        order_id, customer_id, order_status, order_total_amount, currency, 
        created_at, create_date, updated_at, 
        hash_value, source_file, ingestion_ts
    )
    VALUES (
        r.order_id, r.customer_id, r.order_status, r.order_total_amount, r.currency,
        r.created_at, r.create_date, r.updated_at, 
        r.hash_value, r.source_file, CURRENT_TIMESTAMP()
    );
    
    rows_cnt := SQLROWCOUNT;
    proc_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;
    custom_meta := OBJECT_CONSTRUCT(
        'load_strategy', 'MERGE_DEDUP',
        'source_obj',    'BRONZE.orders_stream',
        'business_key',  'order_id',
        'description',   'Deduplication based on updated_at'
    ); 
    CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'SILVER', :PROC_NAME, :PROC_NAME, 'MERGE',
            'SUCCESS', 'Table Merged', :rows_cnt, :error_cnt, :proc_duration, NULL, NULL, :custom_meta
    );
    
    COMMIT;

EXCEPTION
    WHEN OTHER THEN
    
        ROLLBACK;

        LET err_code STRING := SQLCODE;
        LET err_msg STRING := SQLERRM;
        LET err_state STRING := SQLSTATE;

        proc_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;

        LET err_details VARIANT := OBJECT_CONSTRUCT(
            'failed_at_step', :PROC_NAME,
            'sql_state', :err_state
        );

        CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'SILVER', :PROC_NAME, :PROC_NAME, 'FAILURE',
            'FAIL', :err_msg, 0, 0, :proc_duration, :err_code, :err_state, :err_details
        );
        
        RAISE;
END;
$$;






-- ###################################
-- PROC LOAD_SILVER_ORDER_ITEMS
-- ###################################

CREATE OR REPLACE PROCEDURE ECOMMERCE.SILVER.LOAD_ORDER_ITEMS(RUN_ID STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    proc_start_ts TIMESTAMP;
    proc_duration NUMBER(12,3);
    rows_cnt NUMBER;
    error_cnt NUMBER;    
    custom_meta VARIANT;

    PROC_NAME STRING := 'ORDER_ITEMS';

BEGIN
    proc_start_ts := CURRENT_TIMESTAMP();    

    BEGIN TRANSACTION;

    CALL ECOMMERCE.COMMON.LOG_EVENT(
        :RUN_ID, 'SILVER', :PROC_NAME, :PROC_NAME, 'INIT',
        'START', 'Starting load', 0, 0, 0, NULL, NULL, NULL
    );
    

    -- Data Quality Checks
    INSERT INTO ECOMMERCE.COMMON.DQ_ERRORS (
        RUN_ID, LAYER, SOURCE_OBJECT, TARGET_TABLE, RECORD_KEY, RAW_RECORD, ERROR_TYPE, ERROR_MESSAGE
    )
    SELECT 
        :RUN_ID, 
        'SILVER', 
        'order_items_stream', 
        'order_items', 
        payload:order_item_id::STRING, 
        payload, 
        CASE 
            WHEN payload:unit_price::NUMBER(18,2) < 0 THEN 'NEGATIVE_UNIT_PRICE'
            WHEN payload:product_id IS NULL THEN 'MISSING_PRODUCT'
            WHEN payload:created_at::TIMESTAMP_NTZ IS NULL THEN 'MISSING_DATE'
            ELSE 'UNKNOWN'
        END,
        'Validation Failed'
    FROM ECOMMERCE.BRONZE.order_items_stream
    WHERE metadata$action = 'INSERT'
      AND ( 
          payload:unit_price::NUMBER(18,2) < 0 
          OR payload:product_id IS NULL
          OR payload:created_at::TIMESTAMP_NTZ IS NULL
      );

    error_cnt := SQLROWCOUNT;


    MERGE INTO ECOMMERCE.SILVER.order_items s
    USING (
        SELECT
            payload:order_item_id::NUMBER       as order_item_id,
            payload:order_id::NUMBER            as order_id,
            payload:product_id::NUMBER          as product_id,
            payload:quantity::NUMBER            as quantity,
            payload:unit_price::NUMBER(18,2)    as unit_price,
            payload:created_at::TIMESTAMP_NTZ   as created_at,
            CAST(payload:created_at::TIMESTAMP_NTZ AS DATE) as create_date,
    
            SHA2_HEX(CONCAT_WS('|',
                COALESCE(payload:product_id::STRING, ''),
                COALESCE(payload:quantity::STRING, ''),
                COALESCE(payload:unit_price::STRING, '')
            )) AS hash_value,
        
            source_file
        FROM ECOMMERCE.BRONZE.order_items_stream
        WHERE metadata$action = 'INSERT'
          AND NOT (
              payload:unit_price::NUMBER(18,2) < 0 
              OR payload:product_id IS NULL
              OR payload:created_at::TIMESTAMP_NTZ IS NULL
          )
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY payload:order_item_id::NUMBER 
            ORDER BY --payload:updated_at::TIMESTAMP_NTZ DESC,
            ingestion_ts DESC
        ) = 1
    ) r
    ON s.order_item_id = r.order_item_id
    WHEN MATCHED 
        --AND r.updated_at > s.updated_at 
        AND r.hash_value != s.hash_value
    THEN 
        UPDATE SET 
        product_id      = r.product_id,
        quantity        = r.quantity,
        unit_price      = r.unit_price,
        hash_value      = r.hash_value,
        
       -- updated_at      = r.updated_at,
        source_file     = r.source_file,
        ingestion_ts    = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN 
    INSERT (
        order_item_id, order_id, product_id, quantity, unit_price, 
        created_at, create_date, 
        hash_value, source_file, ingestion_ts
    )
    VALUES (
        r.order_item_id, r.order_id, r.product_id, r.quantity, r.unit_price,
        r.created_at, r.create_date, 
        r.hash_value, r.source_file, CURRENT_TIMESTAMP()
    );
     
    rows_cnt := SQLROWCOUNT;
    proc_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;
    custom_meta := OBJECT_CONSTRUCT(
        'load_strategy', 'MERGE_DEDUP',
        'source_obj',    'BRONZE.order_items_stream',
        'business_key',  'order_item_id',
        'description',   'Deduplication based on updated_at'
    ); 
    CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'SILVER', :PROC_NAME, :PROC_NAME, 'MERGE',
            'SUCCESS', 'Table Merged', :rows_cnt, :error_cnt, :proc_duration, NULL, NULL, :custom_meta
    );
    
    COMMIT;

EXCEPTION
    WHEN OTHER THEN
    
        ROLLBACK;

        LET err_code STRING := SQLCODE;
        LET err_msg STRING := SQLERRM;
        LET err_state STRING := SQLSTATE;

        proc_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;

        LET err_details VARIANT := OBJECT_CONSTRUCT(
            'failed_at_step', :PROC_NAME,
            'sql_state', :err_state
        );

        CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'SILVER', :PROC_NAME, :PROC_NAME, 'FAILURE',
            'FAIL', :err_msg, 0, 0, :proc_duration, :err_code, :err_state, :err_details
        );
        
        RAISE;
END;
$$;




-- ###################################
-- PROC LOAD_SILVER_ORDER_STATUS_HISTORY
-- ###################################

CREATE OR REPLACE PROCEDURE ECOMMERCE.SILVER.LOAD_ORDER_STATUS_HISTORY(RUN_ID STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    proc_start_ts TIMESTAMP;
    proc_duration NUMBER(12,3);
    rows_cnt NUMBER;
    error_cnt NUMBER;    
    custom_meta VARIANT;

    PROC_NAME STRING := 'ORDER_STATUS_HISTORY';

BEGIN
    proc_start_ts := CURRENT_TIMESTAMP();    

    BEGIN TRANSACTION;

    CALL ECOMMERCE.COMMON.LOG_EVENT(
        :RUN_ID, 'SILVER', :PROC_NAME, :PROC_NAME, 'INIT',
        'START', 'Starting load', 0, 0, 0, NULL, NULL, NULL
    );
    

    
    -- Data Quality Checks
    INSERT INTO ECOMMERCE.COMMON.DQ_ERRORS (
        RUN_ID, LAYER, SOURCE_OBJECT, TARGET_TABLE, RECORD_KEY, RAW_RECORD, ERROR_TYPE, ERROR_MESSAGE
    )
    SELECT 
        :RUN_ID, 
        'SILVER', 
        'order_status_history_stream', 
        'order_status_history', 
        payload:status_history_id::STRING, 
        payload, 
        CASE 
            WHEN payload:order_id IS NULL THEN 'MISSING_ORDER_ID'
            WHEN payload:status IS NULL THEN 'MISSING_STATUS'
            ELSE 'UNKNOWN'
        END,
        'Validation Failed'
    FROM ECOMMERCE.BRONZE.order_status_history_stream
    WHERE metadata$action = 'INSERT'
      AND ( 
          payload:order_id IS NULL
          OR payload:status IS NULL
      );

    error_cnt := SQLROWCOUNT;



    MERGE INTO ECOMMERCE.SILVER.order_status_history s
    USING (
      SELECT
        payload:status_history_id::NUMBER   as status_history_id,
        payload:order_id::NUMBER            as order_id,
        payload:status::STRING              as status,
        payload:changed_at::TIMESTAMP_NTZ   as changed_at,
        source_file
      FROM ECOMMERCE.BRONZE.order_status_history_stream
    WHERE metadata$action = 'INSERT'
        AND NOT (
          payload:order_id IS NULL
          OR payload:status IS NULL
        )
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY payload:status_history_id::NUMBER 
        ORDER BY payload:changed_at::TIMESTAMP_NTZ DESC, ingestion_ts DESC
      ) = 1 
    ) r
    ON s.status_history_id = r.status_history_id
    WHEN NOT MATCHED THEN 
      INSERT (
        status_history_id, order_id, status, changed_at, source_file
      )
      VALUES (
        r.status_history_id, r.order_id, r.status, r.changed_at, r.source_file
      );
           
    rows_cnt := SQLROWCOUNT;
    proc_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;
    custom_meta := OBJECT_CONSTRUCT(
        'load_strategy', 'MERGE_INSERT',
        'source_obj',    'BRONZE.order_status_history_stream',
        'business_key',  'status_history_id',
        'description',   'Transactional log (idempotent insert, no updates)'
    ); 
    CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'SILVER', :PROC_NAME, :PROC_NAME, 'INSERT',
            'SUCCESS', 'New Events Appended', :rows_cnt, :error_cnt, :proc_duration, NULL, NULL, :custom_meta
    );
    
    COMMIT;

EXCEPTION
    WHEN OTHER THEN
    
        ROLLBACK;

        LET err_code STRING := SQLCODE;
        LET err_msg STRING := SQLERRM;
        LET err_state STRING := SQLSTATE;

        proc_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;

        LET err_details VARIANT := OBJECT_CONSTRUCT(
            'failed_at_step', :PROC_NAME,
            'sql_state', :err_state
        );

        CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'SILVER', :PROC_NAME, :PROC_NAME, 'FAILURE',
            'FAIL', :err_msg, 0, 0, :proc_duration, :err_code, :err_state, :err_details
        );
        
        RAISE;
END;
$$;






-- ###################################
-- PROC LOAD_SILVER_PAYMENTS
-- ###################################

CREATE OR REPLACE PROCEDURE ECOMMERCE.SILVER.LOAD_PAYMENTS(RUN_ID STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    proc_start_ts TIMESTAMP;
    proc_duration NUMBER(12,3);
    rows_cnt NUMBER;
    error_cnt NUMBER;
    custom_meta VARIANT;

    PROC_NAME STRING := 'PAYMENTS';

BEGIN
    proc_start_ts := CURRENT_TIMESTAMP();    

    BEGIN TRANSACTION;

    CALL ECOMMERCE.COMMON.LOG_EVENT(
        :RUN_ID, 'SILVER', :PROC_NAME, :PROC_NAME, 'INIT',
        'START', 'Starting load', 0, 0, 0, NULL, NULL, NULL
    );
    


    -- Data Quality Checks
    INSERT INTO ECOMMERCE.COMMON.DQ_ERRORS (
        RUN_ID, LAYER, SOURCE_OBJECT, TARGET_TABLE, RECORD_KEY, RAW_RECORD, ERROR_TYPE, ERROR_MESSAGE
    )
    SELECT 
        :RUN_ID, 
        'SILVER', 
        'payments_stream', 
        'payments', 
        payload:payment_id::STRING, 
        payload, 
        CASE 
            WHEN payload:payment_status IS NULL THEN 'MISSING_PAYMENT_STATUS'
            WHEN payload:provider IS NULL THEN 'MISSING_PROVIDER'
            WHEN payload:created_at::TIMESTAMP_NTZ IS NULL THEN 'MISSING_DATE'
            ELSE 'UNKNOWN'
        END,
        'Validation Failed'
    FROM ECOMMERCE.BRONZE.payments_stream
    WHERE metadata$action = 'INSERT'
      AND ( 
          payload:payment_status IS NULL
          OR payload:provider IS NULL
          OR payload:created_at::TIMESTAMP_NTZ IS NULL
      );

    error_cnt := SQLROWCOUNT;



    MERGE INTO ECOMMERCE.SILVER.payments s
    USING (
      SELECT
        payload:payment_id::NUMBER          as payment_id,
        payload:order_id::NUMBER            as order_id,
        payload:provider::STRING            as provider,
        payload:payment_status::STRING      as payment_status,
        payload:amount::NUMBER(18,2)        as amount,
        payload:created_at::TIMESTAMP_NTZ   as created_at,
        CAST(payload:created_at::TIMESTAMP_NTZ AS DATE) as create_date,
        payload:updated_at::TIMESTAMP_NTZ   as updated_at,
    
        SHA2_HEX(CONCAT_WS('|',
            COALESCE(payload:provider::STRING, ''),
            COALESCE(payload:payment_status::STRING, ''),
            COALESCE(payload:amount::STRING, '')
        )) AS hash_value,
    
        source_file    
      FROM ECOMMERCE.BRONZE.payments_stream
        WHERE metadata$action = 'INSERT'
          AND NOT (
              payload:payment_status IS NULL
              OR payload:provider IS NULL
              OR payload:created_at::TIMESTAMP_NTZ IS NULL
          )
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY payload:payment_id::NUMBER 
        ORDER BY payload:updated_at::TIMESTAMP_NTZ DESC, ingestion_ts DESC
      ) = 1
    ) r
    ON s.payment_id = r.payment_id
    WHEN MATCHED 
        --AND r.updated_at > s.updated_at 
        AND r.hash_value != s.hash_value
    THEN 
      UPDATE SET 
        order_id        = r.order_id,
        provider        = r.provider,
        payment_status  = r.payment_status,
        amount          = r.amount,
        hash_value      = r.hash_value,
        
        updated_at      = r.updated_at,
        source_file     = r.source_file,
        ingestion_ts    = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN 
      INSERT (
        payment_id, order_id, provider, payment_status, amount, created_at, create_date,
        updated_at, hash_value, source_file, ingestion_ts
      )
      VALUES (
        r.payment_id, r.order_id, r.provider, r.payment_status, r.amount, r.created_at, r.create_date,
        r.updated_at, r.hash_value, r.source_file, CURRENT_TIMESTAMP()
      );
           
    rows_cnt := SQLROWCOUNT;
    proc_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;
    custom_meta := OBJECT_CONSTRUCT(
        'load_strategy', 'MERGE_DEDUP',
        'source_obj',    'BRONZE.payments_stream',
        'business_key',  'payment_id',
        'description',   'Deduplication based on updated_at'
    ); 
    CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'SILVER', :PROC_NAME, :PROC_NAME, 'MERGE',
            'SUCCESS', 'Table Merged', :rows_cnt, :error_cnt, :proc_duration, NULL, NULL, :custom_meta
    );
    
    COMMIT;

EXCEPTION
    WHEN OTHER THEN
    
        ROLLBACK;

        LET err_code STRING := SQLCODE;
        LET err_msg STRING := SQLERRM;
        LET err_state STRING := SQLSTATE;

        proc_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;

        LET err_details VARIANT := OBJECT_CONSTRUCT(
            'failed_at_step', :PROC_NAME,
            'sql_state', :err_state
        );

        CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'SILVER', :PROC_NAME, :PROC_NAME, 'FAILURE',
            'FAIL', :err_msg, 0, 0, :proc_duration, :err_code, :err_state, :err_details
        );
        
        RAISE;
END;
$$;




-- ###################################
-- PROC LOAD_SILVER_SHIPMENTS
-- ###################################

CREATE OR REPLACE PROCEDURE ECOMMERCE.SILVER.LOAD_SHIPMENTS(RUN_ID STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    proc_start_ts TIMESTAMP;
    proc_duration NUMBER(12,3);
    rows_cnt NUMBER;
    error_cnt NUMBER;    
    custom_meta VARIANT;

    PROC_NAME STRING := 'SHIPMENTS';

BEGIN
    proc_start_ts := CURRENT_TIMESTAMP();    

    BEGIN TRANSACTION;

    CALL ECOMMERCE.COMMON.LOG_EVENT(
        :RUN_ID, 'SILVER', :PROC_NAME, :PROC_NAME, 'INIT',
        'START', 'Starting load', 0, 0, 0, NULL, NULL, NULL
    );
    

    -- Data Quality Checks
    INSERT INTO ECOMMERCE.COMMON.DQ_ERRORS (
        RUN_ID, LAYER, SOURCE_OBJECT, TARGET_TABLE, RECORD_KEY, RAW_RECORD, ERROR_TYPE, ERROR_MESSAGE
    )
    SELECT 
        :RUN_ID, 
        'SILVER', 
        'shipments_stream', 
        'shipments', 
        payload:shipment_id::STRING, 
        payload, 
        CASE 
            WHEN payload:carrier IS NULL THEN 'MISSING_CARRIER'
            WHEN payload:shipment_status IS NULL THEN 'MISSING_SHIPMENT_STATUS'
            ELSE 'UNKNOWN'
        END,
        'Validation Failed'
    FROM ECOMMERCE.BRONZE.shipments_stream
    WHERE metadata$action = 'INSERT'
      AND ( 
          payload:carrier IS NULL
          OR payload:shipment_status IS NULL
      );

    error_cnt := SQLROWCOUNT;



    MERGE INTO ECOMMERCE.SILVER.shipments s
    USING (
      SELECT
        payload:shipment_id::NUMBER             as shipment_id,
        payload:order_id::NUMBER                as order_id,
        payload:carrier::STRING                 as carrier,
        payload:shipment_status::STRING         as shipment_status,
        payload:shipped_at::TIMESTAMP_NTZ       as shipped_at,
        CAST(payload:shipped_at::TIMESTAMP_NTZ AS DATE) as ship_date,
        payload:delivered_at::TIMESTAMP_NTZ     as delivered_at,
        CAST(payload:delivered_at::TIMESTAMP_NTZ AS DATE) as delivery_date,
        payload:updated_at::TIMESTAMP_NTZ       as updated_at,
    
        SHA2_HEX(CONCAT_WS('|',
            COALESCE(payload:carrier::STRING, ''),
            COALESCE(payload:shipment_status::STRING, ''),
            COALESCE(payload:shipped_at::TIMESTAMP_NTZ::STRING, '1900-01-01'),
            COALESCE(payload:delivered_at::TIMESTAMP_NTZ::STRING, '1900-01-01')
        )) AS hash_value,
    
        source_file
      FROM ECOMMERCE.BRONZE.shipments_stream
        WHERE metadata$action = 'INSERT'
          AND NOT (
              payload:carrier IS NULL
              OR payload:shipment_status IS NULL
          )
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY payload:shipment_id::NUMBER 
        ORDER BY payload:updated_at::TIMESTAMP_NTZ DESC, ingestion_ts DESC
      ) = 1
    ) r
    ON s.shipment_id = r.shipment_id
    WHEN MATCHED 
        --AND r.updated_at > s.updated_at 
        AND r.hash_value != s.hash_value
    THEN 
      UPDATE SET 
        order_id        = r.order_id,
        carrier         = r.carrier,
        shipment_status = r.shipment_status,
        shipped_at      = r.shipped_at,
        ship_date       = r.ship_date,
        delivered_at    = r.delivered_at,
        delivery_date   = r.delivery_date,
        hash_value      = r.hash_value,
        
        updated_at      = r.updated_at,
        source_file     = r.source_file,
        ingestion_ts    = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN 
      INSERT (
        shipment_id, order_id, carrier, shipment_status, shipped_at, ship_date, delivered_at,
        delivery_date, updated_at, hash_value, source_file, ingestion_ts
      )
      VALUES (
          r.shipment_id, r.order_id, r.carrier, r.shipment_status, r.shipped_at, r.ship_date, r.delivered_at,
          r.delivery_date, r.updated_at, r.hash_value, r.source_file, CURRENT_TIMESTAMP()
      );
           
    rows_cnt := SQLROWCOUNT;
    proc_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;
    custom_meta := OBJECT_CONSTRUCT(
        'load_strategy', 'MERGE_DEDUP',
        'source_obj',    'BRONZE.shipments_stream',
        'business_key',  'shipment_id',
        'description',   'Deduplication based on updated_at'
    ); 
    CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'SILVER', :PROC_NAME, :PROC_NAME, 'MERGE',
            'SUCCESS', 'Table Merged', :rows_cnt, :error_cnt, :proc_duration, NULL, NULL, :custom_meta
    );
    
    COMMIT;

EXCEPTION
    WHEN OTHER THEN
    
        ROLLBACK;

        LET err_code STRING := SQLCODE;
        LET err_msg STRING := SQLERRM;
        LET err_state STRING := SQLSTATE;

        proc_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;

        LET err_details VARIANT := OBJECT_CONSTRUCT(
            'failed_at_step', :PROC_NAME,
            'sql_state', :err_state
        );

        CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'SILVER', :PROC_NAME, :PROC_NAME, 'FAILURE',
            'FAIL', :err_msg, 0, 0, :proc_duration, :err_code, :err_state, :err_details
        );
        
        RAISE;
END;
$$;




-- ###################################
-- PROC LOAD_SILVER_PRODUCTS
-- ###################################

CREATE OR REPLACE PROCEDURE ECOMMERCE.SILVER.LOAD_PRODUCTS(RUN_ID STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    proc_start_ts TIMESTAMP;
    proc_duration NUMBER(12,3);
    rows_cnt NUMBER;
    error_cnt NUMBER;
    custom_meta VARIANT;

    PROC_NAME STRING := 'PRODUCTS';

BEGIN
    proc_start_ts := CURRENT_TIMESTAMP();    

    BEGIN TRANSACTION;

    CALL ECOMMERCE.COMMON.LOG_EVENT(
        :RUN_ID, 'SILVER', :PROC_NAME, :PROC_NAME, 'INIT',
        'START', 'Starting load', 0, 0, 0, NULL, NULL, NULL
    );
    

    -- Data Quality Checks
    INSERT INTO ECOMMERCE.COMMON.DQ_ERRORS (
        RUN_ID, LAYER, SOURCE_OBJECT, TARGET_TABLE, RECORD_KEY, RAW_RECORD, ERROR_TYPE, ERROR_MESSAGE
    )
    SELECT 
        :RUN_ID, 
        'SILVER', 
        'products_stream', 
        'products', 
        payload:product_id::STRING, 
        payload, 
        CASE 
            WHEN payload:price::NUMBER(18,2) < 0 THEN 'NEGATIVE_PRICE'
            WHEN TRIM(payload:name::STRING) = '' OR payload:name IS NULL THEN 'EMPTY_NAME'
            WHEN payload:currency IS NULL THEN 'MISSING_CURRENCY'
            WHEN payload:created_at::TIMESTAMP_NTZ IS NULL THEN 'MISSING_DATE'
            ELSE 'UNKNOWN'
        END,
        'Validation Failed'
    FROM ECOMMERCE.BRONZE.products_stream
    WHERE metadata$action = 'INSERT'
      AND ( 
          payload:price::NUMBER(18,2) < 0 
          OR TRIM(payload:name::STRING) = '' OR payload:name IS NULL
          OR payload:currency IS NULL
          OR payload:created_at::TIMESTAMP_NTZ IS NULL
      );

    error_cnt := SQLROWCOUNT;


    MERGE INTO ECOMMERCE.SILVER.products s
    USING (
      SELECT
        payload:product_id::NUMBER          as product_id,
        payload:name::STRING                as name,
        payload:category::STRING            as category,
        payload:brand::STRING               as brand,
        payload:price::NUMBER(18,2)         as price,
        payload:currency::STRING            as currency,
        payload:status::STRING              as status,
        payload:created_at::TIMESTAMP_NTZ   as created_at,
        CAST(payload:created_at::TIMESTAMP_NTZ AS DATE) as create_date,
        payload:updated_at::TIMESTAMP_NTZ   as updated_at,
        SHA2_HEX(CONCAT_WS('|',
            COALESCE(payload:name::STRING, ''),
            COALESCE(payload:category::STRING, ''),
            COALESCE(payload:brand::STRING, ''),
            COALESCE(payload:price::STRING, '0'),
            COALESCE(payload:currency::STRING, ''),
            COALESCE(payload:status::STRING, '')
        )) AS hash_value,
    
        source_file
      FROM ECOMMERCE.BRONZE.products_stream
    WHERE metadata$action = 'INSERT'
          AND NOT (
              payload:price::NUMBER(18,2) < 0 
              OR payload:currency IS NULL
              OR TRIM(payload:name::STRING) = '' OR payload:name IS NULL
                OR payload:created_at::TIMESTAMP_NTZ IS NULL
          )
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY payload:product_id::NUMBER 
        ORDER BY payload:updated_at::TIMESTAMP_NTZ DESC, ingestion_ts DESC
      ) = 1
    ) r
    ON s.product_id = r.product_id
    WHEN MATCHED 
        --AND r.updated_at > s.updated_at 
        AND r.hash_value != s.hash_value
    THEN 
      UPDATE SET 
        name            = r.name,
        category        = r.category,
        brand           = r.brand,
        price           = r.price,
        currency        = r.currency,
        status          = r.status,
        hash_value      = r.hash_value,
        
        updated_at      = r.updated_at,
        source_file     = r.source_file,
        ingestion_ts    = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN 
      INSERT (
        product_id, name, category, brand, price, currency, status,
        created_at, create_date, updated_at, hash_value, source_file, ingestion_ts
      )
      VALUES (
          r.product_id, r.name, r.category, r.brand, r.price, r.currency, r.status,
          r.created_at, r.create_date, r.updated_at, r.hash_value, r.source_file, CURRENT_TIMESTAMP()
      );
               
    rows_cnt := SQLROWCOUNT;
    proc_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;
    custom_meta := OBJECT_CONSTRUCT(
        'load_strategy', 'MERGE_DEDUP',
        'source_obj',    'BRONZE.products_stream',
        'business_key',  'product_id',
        'description',   'Deduplication based on updated_at'
    ); 
    CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'SILVER', :PROC_NAME, :PROC_NAME, 'MERGE',
            'SUCCESS', 'Table Merged', :rows_cnt, :error_cnt, :proc_duration, NULL, NULL, :custom_meta
    );
    
    COMMIT;

EXCEPTION
    WHEN OTHER THEN
    
        ROLLBACK;

        LET err_code STRING := SQLCODE;
        LET err_msg STRING := SQLERRM;
        LET err_state STRING := SQLSTATE;

        proc_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;

        LET err_details VARIANT := OBJECT_CONSTRUCT(
            'failed_at_step', :PROC_NAME,
            'sql_state', :err_state
        );

        CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'SILVER', :PROC_NAME, :PROC_NAME, 'FAILURE',
            'FAIL', :err_msg, 0, 0, :proc_duration, :err_code, :err_state, :err_details
        );
        
        RAISE;
END;
$$;




-- ###################################
-- PROC LOAD_SILVER_CUSTOMERS
-- ###################################

CREATE OR REPLACE PROCEDURE ECOMMERCE.SILVER.LOAD_CUSTOMERS(RUN_ID STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    proc_start_ts TIMESTAMP;
    proc_duration NUMBER(12,3);
    rows_cnt NUMBER;
    error_cnt NUMBER;
    custom_meta VARIANT;

    PROC_NAME STRING := 'CUSTOMERS';

BEGIN
    proc_start_ts := CURRENT_TIMESTAMP();    

    BEGIN TRANSACTION;

    CALL ECOMMERCE.COMMON.LOG_EVENT(
        :RUN_ID, 'SILVER', :PROC_NAME, :PROC_NAME, 'INIT',
        'START', 'Starting load', 0, 0, 0, NULL, NULL, NULL
    );
    

       
    -- Data Quality Checks
    INSERT INTO ECOMMERCE.COMMON.DQ_ERRORS (
        RUN_ID, LAYER, SOURCE_OBJECT, TARGET_TABLE, RECORD_KEY, RAW_RECORD, ERROR_TYPE, ERROR_MESSAGE
    )
    SELECT 
        :RUN_ID, 
        'SILVER', 
        'customers_stream', 
        'customers', 
        payload:customer_id::STRING, 
        payload, 
        CASE 
            WHEN payload:email::STRING NOT LIKE '%@%' THEN 'INVALID_EMAIL'
            WHEN payload:first_name IS NULL THEN 'MISSING_FIRST_NAME'
            WHEN payload:phone IS NULL THEN 'MISSING_PHONE'
            WHEN payload:created_at::TIMESTAMP_NTZ IS NULL THEN 'MISSING_DATE'
            ELSE 'UNKNOWN'
        END,
        'Validation Failed'
    FROM ECOMMERCE.BRONZE.customers_stream
    WHERE metadata$action = 'INSERT'
      AND ( 
            payload:email::STRING NOT LIKE '%@%' 
            OR payload:first_name IS NULL
            OR payload:phone IS NULL
            OR payload:created_at::TIMESTAMP_NTZ IS NULL
      );

    error_cnt := SQLROWCOUNT;



    MERGE INTO ECOMMERCE.SILVER.customers s
    USING (
      SELECT
        payload:customer_id::NUMBER             as customer_id,
        payload:email::STRING                   as email,
        payload:first_name::STRING              as first_name,
        payload:last_name::STRING               as last_name,
        payload:phone::STRING                   as phone,
        payload:status::STRING                  as status,
        payload:created_at::TIMESTAMP_NTZ       as created_at,
        CAST(payload:created_at::TIMESTAMP_NTZ AS DATE) as create_date,
        payload:updated_at::TIMESTAMP_NTZ       as updated_at,
        SHA2_HEX(CONCAT_WS('|',
            COALESCE(payload:email::STRING, ''),
            COALESCE(payload:first_name::STRING, ''),
            COALESCE(payload:last_name::STRING, ''),
            COALESCE(payload:phone::STRING, ''),
            COALESCE(payload:status::STRING, '')
        )) AS hash_value,
    
        source_file
      FROM ECOMMERCE.BRONZE.customers_stream
        WHERE metadata$action = 'INSERT'
          AND NOT (
              payload:email::STRING NOT LIKE '%@%' 
              OR payload:first_name IS NULL
              OR payload:phone IS NULL
              OR payload:created_at::TIMESTAMP_NTZ IS NULL
          )
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY payload:customer_id::NUMBER 
        ORDER BY payload:updated_at::TIMESTAMP_NTZ DESC, ingestion_ts DESC
      ) = 1
    ) r
    ON s.customer_id = r.customer_id
    WHEN MATCHED 
        --AND r.updated_at > s.updated_at 
        AND r.hash_value != s.hash_value
    THEN 
      UPDATE SET 
        email           = r.email,
        first_name      = r.first_name,
        last_name       = r.last_name,
        phone           = r.phone,
        status          = r.status,
        hash_value      = r.hash_value,
        
        updated_at      = r.updated_at,
        source_file     = r.source_file,
        ingestion_ts    = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN 
      INSERT (
        customer_id, email, first_name, last_name, phone, status,
        created_at, create_date, updated_at, hash_value, source_file, ingestion_ts
      )
      VALUES (
          r.customer_id, r.email, r.first_name, r.last_name, r.phone, r.status, 
          r.created_at, r.create_date, r.updated_at, r.hash_value, r.source_file, CURRENT_TIMESTAMP()
      );
                
    rows_cnt := SQLROWCOUNT;
    proc_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;
    custom_meta := OBJECT_CONSTRUCT(
        'load_strategy', 'MERGE_DEDUP',
        'source_obj',    'BRONZE.customers_stream',
        'business_key',  'customer_id',
        'description',   'Deduplication based on updated_at'
    ); 
    CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'SILVER', :PROC_NAME, :PROC_NAME, 'MERGE',
            'SUCCESS', 'Table Merged', :rows_cnt, :error_cnt, :proc_duration, NULL, NULL, :custom_meta
    );
    
    COMMIT;

EXCEPTION
    WHEN OTHER THEN
    
        ROLLBACK;

        LET err_code STRING := SQLCODE;
        LET err_msg STRING := SQLERRM;
        LET err_state STRING := SQLSTATE;

        proc_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;

        LET err_details VARIANT := OBJECT_CONSTRUCT(
            'failed_at_step', :PROC_NAME,
            'sql_state', :err_state
        );

        CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'SILVER', :PROC_NAME, :PROC_NAME, 'FAILURE',
            'FAIL', :err_msg, 0, 0, :proc_duration, :err_code, :err_state, :err_details
        );
        
        RAISE;
END;
$$;



-- ###################################
-- PROC LOAD_SILVER_ADDRESSES
-- ###################################

CREATE OR REPLACE PROCEDURE ECOMMERCE.SILVER.LOAD_ADDRESSES(RUN_ID STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    proc_start_ts TIMESTAMP;
    proc_duration NUMBER(12,3);
    rows_cnt NUMBER;
    error_cnt NUMBER;
    custom_meta VARIANT;

    PROC_NAME STRING := 'ADDRESSES';

BEGIN
    proc_start_ts := CURRENT_TIMESTAMP();    

    BEGIN TRANSACTION;

    CALL ECOMMERCE.COMMON.LOG_EVENT(
        :RUN_ID, 'SILVER', :PROC_NAME, :PROC_NAME, 'INIT',
        'START', 'Starting load', 0, 0, 0, NULL, NULL, NULL
    );
    

     -- Data Quality Checks
    INSERT INTO ECOMMERCE.COMMON.DQ_ERRORS (
        RUN_ID, LAYER, SOURCE_OBJECT, TARGET_TABLE, RECORD_KEY, RAW_RECORD, ERROR_TYPE, ERROR_MESSAGE
    )
    SELECT 
        :RUN_ID, 
        'SILVER', 
        'addresses_stream', 
        'addresses', 
        payload:address_id::STRING, 
        payload, 
        CASE 
            WHEN payload:city IS NULL THEN 'MISSING_CITY'
            WHEN payload:street IS NULL THEN 'MISSING_STREET'
            WHEN payload:country IS NULL THEN 'MISSING_COUNTRY'
            WHEN payload:customer_id IS NULL THEN 'MISSING_CUSTOMER'
            WHEN payload:created_at::TIMESTAMP_NTZ IS NULL THEN 'MISSING_DATE'
            ELSE 'UNKNOWN'
        END,
        'Validation Failed'
    FROM ECOMMERCE.BRONZE.addresses_stream
    WHERE metadata$action = 'INSERT'
      AND ( 
            payload:city IS NULL
            OR payload:street IS NULL
            OR payload:country IS NULL
            OR payload:customer_id IS NULL
            OR payload:created_at::TIMESTAMP_NTZ IS NULL
      );

    error_cnt := SQLROWCOUNT;




    MERGE INTO ECOMMERCE.SILVER.addresses s
    USING (
      SELECT
        payload:address_id::NUMBER          as address_id,
        payload:customer_id::NUMBER         as customer_id,
        payload:type::STRING                as type,
        payload:street::STRING              as street,
        payload:city::STRING                as city,
        payload:postal_code::STRING         as postal_code,
        payload:country::STRING             as country,
        payload:is_default::BOOLEAN         as is_default,
        payload:created_at::TIMESTAMP_NTZ   as created_at,
        CAST(payload:created_at::TIMESTAMP_NTZ AS DATE) as create_date,
        payload:updated_at::TIMESTAMP_NTZ   as updated_at,
    
        SHA2_HEX(CONCAT_WS('|',
            COALESCE(payload:type::STRING, ''),
            COALESCE(payload:street::STRING, ''),
            COALESCE(payload:city::STRING, ''),
            COALESCE(payload:postal_code::STRING, ''),
            COALESCE(payload:country::STRING, ''),
            COALESCE(payload:is_default::STRING, 'false')
        )) AS hash_value,
    
        source_file
      FROM ECOMMERCE.BRONZE.addresses_stream
    WHERE metadata$action = 'INSERT'
          AND NOT (
                payload:city IS NULL
                OR payload:street IS NULL
                OR payload:country IS NULL
                OR payload:customer_id IS NULL
                OR payload:created_at::TIMESTAMP_NTZ IS NULL
          )
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY payload:address_id::NUMBER 
        ORDER BY payload:updated_at::TIMESTAMP_NTZ DESC, ingestion_ts DESC
      ) = 1
    ) r
    ON s.address_id = r.address_id
    WHEN MATCHED 
        --AND r.updated_at > s.updated_at 
        AND r.hash_value != s.hash_value
    THEN 
      UPDATE SET 
        customer_id     = r.customer_id,
        type            = r.type,
        street          = r.street,
        city            = r.city,
        postal_code     = r.postal_code,
        country         = r.country,
        is_default      = r.is_default,
        hash_value      = r.hash_value,
        
        updated_at      = r.updated_at,
        source_file     = r.source_file,
        ingestion_ts    = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN 
      INSERT (
          address_id, customer_id, type, street, city, postal_code, 
          country, is_default, created_at, create_date, updated_at, 
          hash_value, source_file, ingestion_ts
      )
      VALUES (
          r.address_id, r.customer_id, r.type, r.street, r.city, r.postal_code, 
          r.country, r.is_default, r.created_at, r.create_date, r.updated_at, 
          r.hash_value, r.source_file, CURRENT_TIMESTAMP()
      );
                
    rows_cnt := SQLROWCOUNT;
    proc_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;
    custom_meta := OBJECT_CONSTRUCT(
        'load_strategy', 'MERGE_DEDUP',
        'source_obj',    'BRONZE.addresses_stream',
        'business_key',  'address_id',
        'description',   'Deduplication based on updated_at'
    ); 
    CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'SILVER', :PROC_NAME, :PROC_NAME, 'MERGE',
            'SUCCESS', 'Table Merged', :rows_cnt, :error_cnt, :proc_duration, NULL, NULL, :custom_meta
    );
    
    COMMIT;

EXCEPTION
    WHEN OTHER THEN
    
        ROLLBACK;

        LET err_code STRING := SQLCODE;
        LET err_msg STRING := SQLERRM;
        LET err_state STRING := SQLSTATE;

        proc_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;

        LET err_details VARIANT := OBJECT_CONSTRUCT(
            'failed_at_step', :PROC_NAME,
            'sql_state', :err_state
        );

        CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'SILVER', :PROC_NAME, :PROC_NAME, 'FAILURE',
            'FAIL', :err_msg, 0, 0, :proc_duration, :err_code, :err_state, :err_details
        );
        
        RAISE;
END;
$$;




-- ###################################
-- PROC LOAD_SILVER_MASTER (Cleansed Data to Gold Layer)
-- ###################################

CREATE OR REPLACE PROCEDURE ECOMMERCE.SILVER.LOAD_SILVER_MASTER(RUN_ID STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    proc_start_ts TIMESTAMP;
    total_duration NUMBER(12,3);

BEGIN
    proc_start_ts := CURRENT_TIMESTAMP();

    CALL ECOMMERCE.COMMON.LOG_EVENT(
        :RUN_ID, 'SILVER', 'MASTER', NULL, 'INIT',
        'START', 'Starting Silver Layer', 0, 0, 0, NULL, NULL, NULL
    );

    -- Load individual Silver Layer tables
    -- Tables are loaded in specific order to satisfy FK dependencies
    CALL ECOMMERCE.SILVER.LOAD_PRODUCTS(:RUN_ID);
    CALL ECOMMERCE.SILVER.LOAD_CUSTOMERS(:RUN_ID);
    CALL ECOMMERCE.SILVER.LOAD_ADDRESSES(:RUN_ID);

    CALL ECOMMERCE.SILVER.LOAD_ORDERS(:RUN_ID);
    
    -- Dependency: Requires Silver Orders to be loaded before Fact Sales.
    CALL ECOMMERCE.SILVER.LOAD_ORDER_ITEMS(:RUN_ID);
    CALL ECOMMERCE.SILVER.LOAD_ORDER_STATUS_HISTORY(:RUN_ID);
    CALL ECOMMERCE.SILVER.LOAD_PAYMENTS(:RUN_ID);
    CALL ECOMMERCE.SILVER.LOAD_SHIPMENTS(:RUN_ID);


    -- ###################################
    -- FINISH
    -- ###################################

    total_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;
    CALL ECOMMERCE.COMMON.LOG_EVENT(
        :RUN_ID, 'SILVER', 'MASTER', NULL, 'FINISH',
        'SUCCESS', 'Silver Layer Batch Completed', 0, 0, :total_duration, NULL, NULL, NULL
    );

    RETURN 'SILVER MASTER LOAD COMPLETED SUCCESSFULLY';

EXCEPTION
    WHEN OTHER THEN

        LET err_code STRING := SQLCODE;
        LET err_msg STRING := SQLERRM;
        LET err_state STRING := SQLSTATE;

        total_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;

        LET err_details VARIANT := OBJECT_CONSTRUCT(
            'failed_at_step', 'SILVER MASTER LOAD',
            'sql_state', :err_state
        );

        CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'SILVER', 'MASTER', NULL, 'FAILURE',
            'FAIL', :err_msg, 0, 0, :total_duration, :err_code, :err_state, :err_details
        );

        RAISE;
END;
$$;





-- ##########################################################################################################
-- GOLD LAYER PROCEDURES
-- ##########################################################################################################


-- ###################################
-- PROC LOAD_DIM_PRODUCTS
-- ###################################

CREATE OR REPLACE PROCEDURE ECOMMERCE.GOLD.LOAD_DIM_PRODUCTS(RUN_ID STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    proc_start_ts TIMESTAMP;
    proc_duration NUMBER(12,3);
    rows_cnt NUMBER;
    custom_meta VARIANT;

    PROC_NAME STRING := 'DIM_PRODUCTS';

BEGIN
    proc_start_ts := CURRENT_TIMESTAMP();    

    BEGIN TRANSACTION;

    CALL ECOMMERCE.COMMON.LOG_EVENT(
        :RUN_ID, 'GOLD', :PROC_NAME, :PROC_NAME, 'INIT',
        'START', 'Starting load', 0, 0, 0, NULL, NULL, NULL
    );
    
    MERGE INTO ECOMMERCE.GOLD.dim_products t
    USING (
        SELECT 
            s.product_id, s.name, s.category, s.brand, s.price, s.currency, s.status, s.hash_value,
            s.updated_at AS valid_from, NULL AS join_key
        FROM ECOMMERCE.SILVER.products_stream s
        LEFT JOIN ECOMMERCE.GOLD.dim_products t ON s.product_id = t.product_id AND t.is_current = TRUE
        WHERE t.product_id IS NULL AND s.metadata$action = 'INSERT'
    
        UNION ALL
    
        SELECT 
            s.product_id, s.name, s.category, s.brand, s.price, s.currency, s.status, s.hash_value,
            s.updated_at AS valid_from, s.product_id AS join_key
        FROM ECOMMERCE.SILVER.products_stream s
        JOIN ECOMMERCE.GOLD.dim_products t ON s.product_id = t.product_id AND t.is_current = TRUE
        WHERE s.hash_value != t.hash_value AND s.metadata$action = 'INSERT'
    ) src
    ON t.product_id = src.join_key 
    AND t.is_current = TRUE
    
    WHEN MATCHED THEN 
        UPDATE SET 
            t.valid_to = src.valid_from,
            t.is_current = FALSE
            
    WHEN NOT MATCHED THEN
        INSERT (product_id, name, category, brand, price, currency,
                status, hash_value, valid_from, valid_to, is_current)
        VALUES (src.product_id, src.name, src.category, src.brand, src.price, src.currency,
                src.status, src.hash_value, src.valid_from, '9999-12-31', TRUE);
    
    rows_cnt := SQLROWCOUNT;
    proc_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;
    custom_meta := OBJECT_CONSTRUCT(
        'load_strategy', 'MERGE_SCD2',
        'source_obj',    'SILVER.products_stream',
        'business_key',  'product_id',
        'description',   'Tracking price/status changes history'
    ); 
    CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'GOLD', :PROC_NAME, :PROC_NAME, 'MERGE',
            'SUCCESS', 'Dimension History Updated', :rows_cnt, 0, :proc_duration, NULL, NULL, :custom_meta
    );
    
    COMMIT;

EXCEPTION
    WHEN OTHER THEN
    
        ROLLBACK;

        LET err_code STRING := SQLCODE;
        LET err_msg STRING := SQLERRM;
        LET err_state STRING := SQLSTATE;

        proc_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;

        LET err_details VARIANT := OBJECT_CONSTRUCT(
            'failed_at_step', :PROC_NAME,
            'sql_state', :err_state
        );

        CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'GOLD', :PROC_NAME, :PROC_NAME, 'FAILURE',
            'FAIL', :err_msg, 0, 0, :proc_duration, :err_code, :err_state, :err_details
        );
        
        RAISE;
END;
$$;



-- ###################################
-- PROC LOAD_DIM_CUSTOMERS
-- ###################################

CREATE OR REPLACE PROCEDURE ECOMMERCE.GOLD.LOAD_DIM_CUSTOMERS(RUN_ID STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    proc_start_ts TIMESTAMP;
    proc_duration NUMBER(12,3);
    rows_cnt NUMBER;
    custom_meta VARIANT;

    PROC_NAME STRING := 'DIM_CUSTOMERS';

BEGIN
    proc_start_ts := CURRENT_TIMESTAMP();    

    BEGIN TRANSACTION;

    CALL ECOMMERCE.COMMON.LOG_EVENT(
        :RUN_ID, 'GOLD', :PROC_NAME, :PROC_NAME, 'INIT',
        'START', 'Starting load', 0, 0, 0, NULL, NULL, NULL
    );
    

    MERGE INTO ECOMMERCE.GOLD.dim_customers t
    USING (
        -- Get the IDs of all customers who have profile OR address changes
        WITH changed_ids AS (
            SELECT customer_id FROM ECOMMERCE.SILVER.customers_stream WHERE metadata$action = 'INSERT'
            UNION
            SELECT customer_id FROM ECOMMERCE.SILVER.addresses_stream WHERE metadata$action = 'INSERT'
        ),
        -- Retrieve the current, full record for these customers from the SILVER tables
        source_data AS (
            SELECT 
                c.customer_id, c.email, c.first_name, c.last_name, c.phone, c.status,
                COALESCE(a.city, 'Unknown') as city,
                COALESCE(a.country, 'Unknown') as country,
                COALESCE(a.postal_code, 'Unknown') as postal_code,
                COALESCE(a.street, 'Unknown') as street,
                COALESCE(a.type, 'Unknown') as address_type,
    
                SHA2_HEX(CONCAT_WS('|',
                    COALESCE(c.email, ''),
                    COALESCE(c.first_name, ''),
                    COALESCE(c.last_name, ''),
                    COALESCE(c.phone, ''),
                    COALESCE(c.status, ''),
                    COALESCE(a.city, ''),
                    COALESCE(a.country, ''),
                    COALESCE(a.postal_code, ''),
                    COALESCE(a.street, ''),
                    COALESCE(a.type, '')
                )) as hash_value,
                
                CURRENT_TIMESTAMP() as updated_at
            FROM ECOMMERCE.SILVER.customers c
            JOIN changed_ids ch ON c.customer_id = ch.customer_id
            LEFT JOIN ECOMMERCE.SILVER.addresses a 
                ON c.customer_id = a.customer_id AND a.is_default = TRUE
        )
        -- Select the data for new customers
        SELECT 
            s.customer_id, s.email, s.first_name, s.last_name, s.phone, s.status,
            s.city, s.country, s.postal_code, s.street, s.address_type,
            s.hash_value,
            s.updated_at as valid_from, 
            NULL as join_key
        FROM source_data s
        LEFT JOIN ECOMMERCE.GOLD.dim_customers t 
          ON s.customer_id = t.customer_id AND t.is_current = TRUE
        WHERE t.customer_id IS NULL
        
        UNION ALL
        
        -- Select the data for customers with changed data
        SELECT 
            s.customer_id, s.email, s.first_name, s.last_name, s.phone, s.status,
            s.city, s.country, s.postal_code, s.street, s.address_type,
            s.hash_value,
            s.updated_at as valid_from, 
            s.customer_id as join_key
        FROM source_data s
        JOIN ECOMMERCE.GOLD.dim_customers t 
          ON s.customer_id = t.customer_id 
          AND t.is_current = TRUE
        WHERE s.hash_value != t.hash_value
    ) src
    -- Join the source data with the existing records in the GOLD table
    ON t.customer_id = src.join_key AND t.is_current = TRUE
    WHEN MATCHED THEN 
        UPDATE SET 
        t.valid_to = src.valid_from,
        t.is_current = FALSE
    WHEN NOT MATCHED THEN
        INSERT (customer_id, email, first_name, last_name, phone, status, 
                city, country, postal_code, street, address_type,
                hash_value, valid_from, valid_to, is_current)
        VALUES (src.customer_id, src.email, src.first_name, src.last_name, src.phone, src.status,
                src.city, src.country, src.postal_code, src.street, src.address_type,
                src.hash_value, src.valid_from, '9999-12-31', TRUE);
        
    rows_cnt := SQLROWCOUNT;
    proc_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;
    custom_meta := OBJECT_CONSTRUCT(
        'load_strategy', 'MERGE_SCD2',
        'source_obj',    'SILVER.customers_stream, SILVER.addresses',
        'business_key',  'customer_id',
        'description',   'Tracking email/address changes history'
    ); 
    CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'GOLD', :PROC_NAME, :PROC_NAME, 'MERGE',
            'SUCCESS', 'Dimension History Updated', :rows_cnt, 0, :proc_duration, NULL, NULL, :custom_meta
    );

    COMMIT;

EXCEPTION
    WHEN OTHER THEN
    
        ROLLBACK;

        LET err_code STRING := SQLCODE;
        LET err_msg STRING := SQLERRM;
        LET err_state STRING := SQLSTATE;

        proc_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;

        LET err_details VARIANT := OBJECT_CONSTRUCT(
            'failed_at_step', :PROC_NAME,
            'sql_state', :err_state
        );

        CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'GOLD', :PROC_NAME, :PROC_NAME, 'FAILURE',
            'FAIL', :err_msg, 0, 0, :proc_duration, :err_code, :err_state, :err_details
        );
        
        RAISE;
END;
$$;




-- ###################################
-- PROC LOAD_FACT_ORDERS
-- ###################################

CREATE OR REPLACE PROCEDURE ECOMMERCE.GOLD.LOAD_FACT_ORDERS(RUN_ID STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    proc_start_ts TIMESTAMP;
    proc_duration NUMBER(12,3);
    rows_cnt NUMBER;
    custom_meta VARIANT;

    PROC_NAME STRING := 'FACT_ORDERS';

BEGIN
    proc_start_ts := CURRENT_TIMESTAMP();    

    BEGIN TRANSACTION;

    CALL ECOMMERCE.COMMON.LOG_EVENT(
        :RUN_ID, 'GOLD', :PROC_NAME, :PROC_NAME, 'INIT',
        'START', 'Starting load', 0, 0, 0, NULL, NULL, NULL
    );
    
    MERGE INTO ECOMMERCE.GOLD.fact_orders t
    USING (
        SELECT 
            o.order_id,
            COALESCE(dc.customer_key, -1) as customer_key,
            
            TO_NUMBER(TO_CHAR(o.create_date, 'YYYYMMDD')) as order_date_key,
            
            o.order_status,
            o.order_total_amount,
            o.currency,
            o.created_at,
            o.updated_at
        FROM ECOMMERCE.SILVER.orders_stream o
        LEFT JOIN ECOMMERCE.GOLD.dim_customers dc 
            ON o.customer_id = dc.customer_id 
            AND o.created_at >= dc.valid_from 
            AND o.created_at < dc.valid_to
        WHERE o.metadata$action = 'INSERT'
    ) src
    ON t.order_id = src.order_id
    WHEN MATCHED THEN
        UPDATE SET 
            t.order_status = src.order_status,
            t.order_total_amount = src.order_total_amount,
            t.currency = src.currency,
            t.updated_at = src.updated_at,
            t.customer_key = src.customer_key,
            t.ingestion_ts = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (order_id, customer_key, order_date_key, order_status, order_total_amount, currency, created_at, updated_at)
        VALUES (src.order_id, src.customer_key, src.order_date_key, src.order_status, src.order_total_amount, src.currency, src.created_at, src.updated_at);
        
    rows_cnt := SQLROWCOUNT;
    proc_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;
    custom_meta := OBJECT_CONSTRUCT(
        'load_strategy', 'MERGE_FACT',
        'source_obj',    'SILVER.orders_stream',
        'business_key',  'order_id',
        'description',   'Accumulating snapshot of orders (updates allowed)'
    ); 
    CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'GOLD', :PROC_NAME, :PROC_NAME, 'MERGE',
            'SUCCESS', 'Fact Table Merged', :rows_cnt, 0, :proc_duration, NULL, NULL, :custom_meta
    );
    
    COMMIT;

EXCEPTION
    WHEN OTHER THEN
    
        ROLLBACK;

        LET err_code STRING := SQLCODE;
        LET err_msg STRING := SQLERRM;
        LET err_state STRING := SQLSTATE;

        proc_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;

        LET err_details VARIANT := OBJECT_CONSTRUCT(
            'failed_at_step', :PROC_NAME,
            'sql_state', :err_state
        );

        CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'GOLD', :PROC_NAME, :PROC_NAME, 'FAILURE',
            'FAIL', :err_msg, 0, 0, :proc_duration, :err_code, :err_state, :err_details
        );
        
        RAISE;
END;
$$;





-- ###################################
-- PROC LOAD_FACT_SALES
-- ###################################

CREATE OR REPLACE PROCEDURE ECOMMERCE.GOLD.LOAD_FACT_SALES(RUN_ID STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    proc_start_ts TIMESTAMP;
    proc_duration NUMBER(12,3);
    rows_cnt NUMBER;
    custom_meta VARIANT;

    PROC_NAME STRING := 'FACT_SALES';

BEGIN
    proc_start_ts := CURRENT_TIMESTAMP();    

    BEGIN TRANSACTION;

    CALL ECOMMERCE.COMMON.LOG_EVENT(
        :RUN_ID, 'GOLD', :PROC_NAME, :PROC_NAME, 'INIT',
        'START', 'Starting load', 0, 0, 0, NULL, NULL, NULL
    );
    
    MERGE INTO ECOMMERCE.GOLD.fact_sales t
    USING (
        SELECT 
            oi.order_item_id,
            oi.order_id,
            
            COALESCE(dp.product_key, -1) as product_key,
            COALESCE(dc.customer_key, -1) as customer_key,
            TO_NUMBER(TO_CHAR(oi.create_date, 'YYYYMMDD')) as order_date_key,
            
            oi.quantity,
            oi.unit_price,
            (oi.quantity * oi.unit_price) as total_line_amount,
            o.currency
            
        FROM ECOMMERCE.SILVER.order_items_stream oi
        LEFT JOIN ECOMMERCE.SILVER.orders o ON oi.order_id = o.order_id
        
        LEFT JOIN ECOMMERCE.GOLD.dim_products dp 
            ON oi.product_id = dp.product_id 
            AND oi.created_at >= dp.valid_from 
            AND oi.created_at < dp.valid_to
            
        LEFT JOIN ECOMMERCE.GOLD.dim_customers dc 
            ON o.customer_id = dc.customer_id 
            AND o.created_at >= dc.valid_from 
            AND o.created_at < dc.valid_to
            
        WHERE oi.metadata$action = 'INSERT'
    ) src
    ON t.order_item_id = src.order_item_id
    WHEN MATCHED THEN
        UPDATE SET 
            t.product_key = src.product_key,
            t.customer_key = src.customer_key,
            t.quantity = src.quantity,
            t.unit_price = src.unit_price,
            t.total_line_amount = src.total_line_amount,
            t.ingestion_ts = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (order_item_id, order_id, product_key, customer_key, order_date_key, 
                quantity, unit_price, total_line_amount, currency)
        VALUES (src.order_item_id, src.order_id, src.product_key, src.customer_key, src.order_date_key,
                src.quantity, src.unit_price, src.total_line_amount, src.currency);       
            
    rows_cnt := SQLROWCOUNT;
    proc_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;
    custom_meta := OBJECT_CONSTRUCT(
        'load_strategy', 'MERGE_FACT',
        'source_obj',    'SILVER.order_items_stream',
        'business_key',  'order_item_id',
        'description',   'Accumulating snapshot of sales (updates allowed)'
    ); 
    CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'GOLD', :PROC_NAME, :PROC_NAME, 'MERGE',
            'SUCCESS', 'Fact Table Merged', :rows_cnt, 0, :proc_duration, NULL, NULL, :custom_meta
    );
    
    COMMIT;

EXCEPTION
    WHEN OTHER THEN
    
        ROLLBACK;

        LET err_code STRING := SQLCODE;
        LET err_msg STRING := SQLERRM;
        LET err_state STRING := SQLSTATE;

        proc_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;

        LET err_details VARIANT := OBJECT_CONSTRUCT(
            'failed_at_step', :PROC_NAME,
            'sql_state', :err_state
        );

        CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'GOLD', :PROC_NAME, :PROC_NAME, 'FAILURE',
            'FAIL', :err_msg, 0, 0, :proc_duration, :err_code, :err_state, :err_details
        );
        
        RAISE;
END;
$$;






-- ###################################
-- PROC LOAD_FACT_SHIPMENTS
-- ###################################

CREATE OR REPLACE PROCEDURE ECOMMERCE.GOLD.LOAD_FACT_SHIPMENTS(RUN_ID STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    proc_start_ts TIMESTAMP;
    proc_duration NUMBER(12,3);
    rows_cnt NUMBER;
    custom_meta VARIANT;

    PROC_NAME STRING := 'FACT_SHIPMENTS';

BEGIN
    proc_start_ts := CURRENT_TIMESTAMP();    

    BEGIN TRANSACTION;

    CALL ECOMMERCE.COMMON.LOG_EVENT(
        :RUN_ID, 'GOLD', :PROC_NAME, :PROC_NAME, 'INIT',
        'START', 'Starting load', 0, 0, 0, NULL, NULL, NULL
    );
    
    MERGE INTO ECOMMERCE.GOLD.fact_shipments t
    USING (
        SELECT 
            s.shipment_id,
            s.order_id,
            
            COALESCE(dc.customer_key, -1) as customer_key,
            IFF(s.ship_date IS NULL, -1, TO_NUMBER(TO_CHAR(s.ship_date, 'YYYYMMDD'))) as ship_date_key,
            IFF(s.delivery_date IS NULL, -1, TO_NUMBER(TO_CHAR(s.delivery_date, 'YYYYMMDD'))) as delivery_date_key,
            s.shipped_at,
            s.delivered_at,
            
            s.carrier,
            s.shipment_status,
            
            DATEDIFF(day, o.created_at, s.shipped_at) as days_to_ship,
            DATEDIFF(day, s.shipped_at, s.delivered_at) as days_to_deliver
            
        FROM ECOMMERCE.SILVER.shipments_stream s
        LEFT JOIN ECOMMERCE.SILVER.orders o ON s.order_id = o.order_id
        
        LEFT JOIN ECOMMERCE.GOLD.dim_customers dc 
            ON o.customer_id = dc.customer_id 
            AND s.updated_at >= dc.valid_from 
            AND s.updated_at < dc.valid_to
            
        WHERE s.metadata$action = 'INSERT'
    ) src
    ON t.shipment_id = src.shipment_id
    WHEN MATCHED THEN
        UPDATE SET 
            t.shipment_status = src.shipment_status,
            t.ship_date_key = src.ship_date_key,
            t.delivery_date_key = src.delivery_date_key,
            t.days_to_ship = src.days_to_ship,
            t.days_to_deliver = src.days_to_deliver,
            t.customer_key = src.customer_key,
            t.shipped_at = src.shipped_at,
            t.delivered_at = src.delivered_at,
            t.ingestion_ts = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (shipment_id, order_id, customer_key, ship_date_key, delivery_date_key,
            shipped_at, delivered_at, carrier, shipment_status, days_to_ship, days_to_deliver)
        VALUES (src.shipment_id, src.order_id, src.customer_key, src.ship_date_key, src.delivery_date_key,
            src.shipped_at, src.delivered_at, src.carrier, src.shipment_status, src.days_to_ship, src.days_to_deliver);
                
    rows_cnt := SQLROWCOUNT;
    proc_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;
    custom_meta := OBJECT_CONSTRUCT(
        'load_strategy', 'MERGE_FACT',
        'source_obj',    'SILVER.shipments_stream',
        'business_key',  'shipment_id',
        'description',   'Accumulating snapshot of shipments (updates allowed)'
    ); 
    CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'GOLD', :PROC_NAME, :PROC_NAME, 'MERGE',
            'SUCCESS', 'Fact Table Merged', :rows_cnt, 0, :proc_duration, NULL, NULL, :custom_meta
    );
    
    COMMIT;

EXCEPTION
    WHEN OTHER THEN
    
        ROLLBACK;

        LET err_code STRING := SQLCODE;
        LET err_msg STRING := SQLERRM;
        LET err_state STRING := SQLSTATE;

        proc_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;

        LET err_details VARIANT := OBJECT_CONSTRUCT(
            'failed_at_step', :PROC_NAME,
            'sql_state', :err_state
        );

        CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'GOLD', :PROC_NAME, :PROC_NAME, 'FAILURE',
            'FAIL', :err_msg, 0, 0, :proc_duration, :err_code, :err_state, :err_details
        );
        
        RAISE;
END;
$$;





-- ###################################
-- PROC LOAD_FACT_PAYMENTS
-- ###################################

CREATE OR REPLACE PROCEDURE ECOMMERCE.GOLD.LOAD_FACT_PAYMENTS(RUN_ID STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    proc_start_ts TIMESTAMP;
    proc_duration NUMBER(12,3);
    rows_cnt NUMBER;
    custom_meta VARIANT;

    PROC_NAME STRING := 'FACT_PAYMENTS';

BEGIN
    proc_start_ts := CURRENT_TIMESTAMP();    

    BEGIN TRANSACTION;

    CALL ECOMMERCE.COMMON.LOG_EVENT(
        :RUN_ID, 'GOLD', :PROC_NAME, :PROC_NAME, 'INIT',
        'START', 'Starting load', 0, 0, 0, NULL, NULL, NULL
    );
    
    MERGE INTO ECOMMERCE.GOLD.fact_payments t
    USING (
        SELECT 
            p.payment_id,
            p.order_id,
            
            COALESCE(dc.customer_key, -1) as customer_key,
            
            TO_NUMBER(TO_CHAR(p.create_date, 'YYYYMMDD')) as date_key,
            p.provider,
            p.payment_status,
            p.amount
            
        FROM ECOMMERCE.SILVER.payments_stream p
        LEFT JOIN ECOMMERCE.SILVER.orders o ON p.order_id = o.order_id
        
        LEFT JOIN ECOMMERCE.GOLD.dim_customers dc 
            ON o.customer_id = dc.customer_id 
            AND p.created_at >= dc.valid_from 
            AND p.created_at < dc.valid_to
            
        WHERE p.metadata$action = 'INSERT'
    ) src
    ON t.payment_id = src.payment_id
    WHEN MATCHED THEN
        UPDATE SET 
            t.payment_status = src.payment_status,
            t.amount = src.amount,
            t.provider = src.provider,
            t.customer_key = src.customer_key,
            t.ingestion_ts = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (payment_id, order_id, customer_key, date_key, provider, payment_status, amount)
        VALUES (src.payment_id, src.order_id, src.customer_key, src.date_key, src.provider, src.payment_status, src.amount);
                                
    rows_cnt := SQLROWCOUNT;
    proc_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;
    custom_meta := OBJECT_CONSTRUCT(
        'load_strategy', 'MERGE_FACT',
        'source_obj',    'SILVER.payments_stream',
        'business_key',  'payment_id',
        'description',   'Accumulating snapshot of payments (updates allowed)'
    ); 
    CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'GOLD', :PROC_NAME, :PROC_NAME, 'MERGE',
            'SUCCESS', 'Fact Table Merged', :rows_cnt, 0, :proc_duration, NULL, NULL, :custom_meta
    );
    
    COMMIT;

EXCEPTION
    WHEN OTHER THEN
    
        ROLLBACK;

        LET err_code STRING := SQLCODE;
        LET err_msg STRING := SQLERRM;
        LET err_state STRING := SQLSTATE;

        proc_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;

        LET err_details VARIANT := OBJECT_CONSTRUCT(
            'failed_at_step', :PROC_NAME,
            'sql_state', :err_state
        );

        CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'GOLD', :PROC_NAME, :PROC_NAME, 'FAILURE',
            'FAIL', :err_msg, 0, 0, :proc_duration, :err_code, :err_state, :err_details
        );
        
        RAISE;
END;
$$;




-- ###################################
-- PROC LOAD_FACT_ORDER_STATUS_HISTORY
-- ###################################

CREATE OR REPLACE PROCEDURE ECOMMERCE.GOLD.LOAD_FACT_ORDER_STATUS_HISTORY(RUN_ID STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    proc_start_ts TIMESTAMP;
    proc_duration NUMBER(12,3);
    rows_cnt NUMBER;
    custom_meta VARIANT;

    PROC_NAME STRING := 'FACT_ORDER_STATUS_HISTORY';

BEGIN
    proc_start_ts := CURRENT_TIMESTAMP();    

    BEGIN TRANSACTION;

    CALL ECOMMERCE.COMMON.LOG_EVENT(
        :RUN_ID, 'GOLD', :PROC_NAME, :PROC_NAME, 'INIT',
        'START', 'Starting load', 0, 0, 0, NULL, NULL, NULL
    );
    
    MERGE INTO ECOMMERCE.GOLD.fact_order_status_history t
    USING (
        SELECT 
            h.status_history_id,
            h.order_id,
            TO_NUMBER(TO_CHAR(h.changed_at, 'YYYYMMDD')) as date_key,
            h.changed_at,
            h.status
        FROM ECOMMERCE.SILVER.order_status_history_stream h
        WHERE h.metadata$action = 'INSERT'
    ) src
    ON t.status_history_id = src.status_history_id
    WHEN NOT MATCHED THEN
        INSERT (status_history_id, order_id, date_key, changed_at_ts, status)
        VALUES (src.status_history_id, src.order_id, src.date_key, src.changed_at, src.status);
                                
    rows_cnt := SQLROWCOUNT;
    proc_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;
    custom_meta := OBJECT_CONSTRUCT(
        'load_strategy', 'MERGE_INSERT',
        'source_obj',    'SILVER.order_status_history_stream',
        'business_key',  'status_history_id',
        'description',   'Transactional log (idempotent insert, no updates)'
    ); 
    CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'GOLD', :PROC_NAME, :PROC_NAME, 'INSERT',
            'SUCCESS', 'New Facts Appended', :rows_cnt, 0, :proc_duration, NULL, NULL, :custom_meta
    );
    
    COMMIT;

EXCEPTION
    WHEN OTHER THEN
    
        ROLLBACK;

        LET err_code STRING := SQLCODE;
        LET err_msg STRING := SQLERRM;
        LET err_state STRING := SQLSTATE;

        proc_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;

        LET err_details VARIANT := OBJECT_CONSTRUCT(
            'failed_at_step', :PROC_NAME,
            'sql_state', :err_state
        );

        CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'GOLD', :PROC_NAME, :PROC_NAME, 'FAILURE',
            'FAIL', :err_msg, 0, 0, :proc_duration, :err_code, :err_state, :err_details
        );
        
        RAISE;
END;
$$;





-- ###################################
-- PROC LOAD_GOLD_MASTER (Dimensional modeling)
-- ###################################

CREATE OR REPLACE PROCEDURE ECOMMERCE.GOLD.LOAD_GOLD_MASTER(RUN_ID STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    proc_start_ts TIMESTAMP;
    total_duration NUMBER(12,3);

BEGIN
    proc_start_ts := CURRENT_TIMESTAMP();

    CALL ECOMMERCE.COMMON.LOG_EVENT(
        :RUN_ID, 'GOLD', 'MASTER', NULL, 'INIT',
        'START', 'Starting Gold Layer Batch', 0, 0, 0, NULL, NULL, NULL
    );

    -- Load individual Gold Layer tables
    -- Tables are loaded in specific order to satisfy FK dependencies
    CALL ECOMMERCE.GOLD.LOAD_DIM_PRODUCTS(:RUN_ID);
    CALL ECOMMERCE.GOLD.LOAD_DIM_CUSTOMERS(:RUN_ID);

    CALL ECOMMERCE.GOLD.LOAD_FACT_ORDERS(:RUN_ID);
    
    CALL ECOMMERCE.GOLD.LOAD_FACT_SALES(:RUN_ID);
    CALL ECOMMERCE.GOLD.LOAD_FACT_SHIPMENTS(:RUN_ID);
    CALL ECOMMERCE.GOLD.LOAD_FACT_PAYMENTS(:RUN_ID);
    CALL ECOMMERCE.GOLD.LOAD_FACT_ORDER_STATUS_HISTORY(:RUN_ID);


    -- ###################################
    -- FINISH
    -- ###################################

    total_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;
    CALL ECOMMERCE.COMMON.LOG_EVENT(
        :RUN_ID, 'GOLD', 'MASTER', NULL, 'FINISH',
        'SUCCESS', 'Gold Layer Batch Completed', 0, 0, :total_duration, NULL, NULL, NULL
    );

    RETURN 'GOLD MASTER LOAD COMPLETED SUCCESSFULLY';

EXCEPTION
    WHEN OTHER THEN

        LET err_code STRING := SQLCODE;
        LET err_msg STRING := SQLERRM;
        LET err_state STRING := SQLSTATE;

        total_duration := DATEDIFF('millisecond', :proc_start_ts, CURRENT_TIMESTAMP()) / 1000;

        LET err_details VARIANT := OBJECT_CONSTRUCT(
            'failed_at_step', 'GOLD MASTER LOAD',
            'sql_state', :err_state
        );

        CALL ECOMMERCE.COMMON.LOG_EVENT(
            :RUN_ID, 'GOLD', 'MASTER', NULL, 'FAILURE',
            'FAIL', :err_msg, 0, 0, :total_duration, :err_code, :err_state, :err_details
        );

        RAISE;
END;
$$;