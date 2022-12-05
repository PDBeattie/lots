with pre_clean as (select QUERY_ID
                        , replace(QUERY_TEXT, '"', '')                         as QUERY_TEXT
                        , QUERY_TYPE
                        , QUERY_LOAD_PERCENT
                        , USER_NAME
                        , WAREHOUSE_SIZE
                        , TOTAL_ELAPSED_TIME / 1000                            as TOTAL_ELAPSED_TIME
                        , COMPILATION_TIME / 1000                              as COMPILATION_TIME
                        , EXECUTION_TIME / 1000                                as EXECUTION_TIME
                        , QUEUED_PROVISIONING_TIME / 1000                      as QUEUED_PROVISIONING_TIME
                        , QUEUED_OVERLOAD_TIME / 1000                          as QUEUED_OVERLOAD_TIME
                        , QUEUED_REPAIR_TIME / 1000                            as QUEUED_REPAIR_TIME
                        , PERCENTAGE_SCANNED_FROM_CACHE                        as PERCENTAGE_SCANNED_FROM_CACHE
                        , BYTES_SCANNED / 1024 / 1024 / 1024                   as GB_SCANNED
                        , BYTES_READ_FROM_RESULT / 1024 / 1024 / 1024          as GB_READ_FROM_RESULT
                        , BYTES_SENT_OVER_THE_NETWORK / 1024 / 1024 / 1024     as GB_SENT_OVER_THE_NETWORK
                        , BYTES_SPILLED_TO_LOCAL_STORAGE / 1024 / 1024 / 1024  as GB_SPILLED_TO_LOCAL_STORAGE
                        , BYTES_SPILLED_TO_REMOTE_STORAGE / 1024 / 1024 / 1024 as GB_SPILLED_TO_REMOTE_STORAGE
                        , BYTES_WRITTEN / 1024 / 1024 / 1024                   as GB_WRITTEN
                        , BYTES_WRITTEN_TO_RESULT / 1024 / 1024 / 1024         as GB_WRITTEN_TO_RESULT
                        , ROWS_PRODUCED
                        , ROWS_INSERTED
                        , ROWS_DELETED
                        , ROWS_UPDATED
                        , ROWS_UNLOADED
                        , EXECUTION_STATUS
                   from META.SF_INFO.QUERY_HISTORY
                   where WAREHOUSE_NAME in ('ANALYST_WH', 'ANALYST_WH2')
                     and DATETIME_START_UTC::date between '2022-11-01' and '2022-11-05'
                     and PERCENTAGE_SCANNED_FROM_CACHE < 0.2
                     and USER_NAME ilike '%vestiairecollective%'
)

select QUERY_ID
     , QUERY_TEXT
     , regexp_substr_all(QUERY_TEXT, 'join', 1, 1, 'i')              as join_list
     , regexp_substr_all(
        QUERY_TEXT,
        '(dwh_|data_science|analyst_temp_storage|tradesy|viz_tableau)[^\\\s]+',
        1,
        1,
        'i')                                                         as tables_scan_list
     , regexp_substr_all(QUERY_TEXT, 'group by|distinct', 1, 1, 'i') as aggregate_list
     , QUERY_TYPE
     , QUERY_LOAD_PERCENT
     , USER_NAME
     , WAREHOUSE_SIZE
     , TOTAL_ELAPSED_TIME
     , COMPILATION_TIME
     , EXECUTION_TIME
     , QUEUED_PROVISIONING_TIME
     , QUEUED_OVERLOAD_TIME
     , QUEUED_REPAIR_TIME
     , PERCENTAGE_SCANNED_FROM_CACHE
     , GB_SCANNED
     , GB_READ_FROM_RESULT
     , GB_SENT_OVER_THE_NETWORK
     , GB_SPILLED_TO_LOCAL_STORAGE
     , GB_SPILLED_TO_REMOTE_STORAGE
     , GB_WRITTEN
     , GB_WRITTEN_TO_RESULT
     , ROWS_PRODUCED
     , ROWS_INSERTED
     , ROWS_DELETED
     , ROWS_UPDATED
     , ROWS_UNLOADED
     , EXECUTION_STATUS
from pre_clean;