with
    patterns as (select * from {project_name}. {audit_dataset_name}.raw_table_latency_thresholds),

    -- Get dataset labels as JSON
    dataset_labels as (
        select schema_name, option_name, option_value as labels_json
        from `{project_name}.region-us-central1.INFORMATION_SCHEMA.SCHEMATA_OPTIONS` dl
        where option_name = 'labels' and option_value like '%"latency_check_ignore", "true"%'
    ),

    -- Get table labels as JSON
    table_labels as (
        select distinct table_schema, table_name from {project_name}. {audit_dataset_name}.ignore_latency_tables_list
    ),

    matched_tables as (
        select distinct
            ts.table_schema,
            ts.table_name,
            ts.storage_last_modified_time,
            p.latency_threshold,
            timestamp_diff(current_timestamp(), ts.storage_last_modified_time, hour) as hours_since_last_update,
            p.source,
            p.table as internal_table_name,
            p.table_pattern
        from `{project_name}.region-us-central1.INFORMATION_SCHEMA.TABLE_STORAGE` ts
        cross join patterns p
        where
            ts.table_schema like '%_prod_raw'
            and lower(ts.table_name) like p.table_pattern
            and p.latency_threshold is not null
            and ts.storage_last_modified_time < timestamp_sub(current_timestamp(), interval p.latency_threshold hour)
            -- Exclude tables where dataset labels contain 'latency_check_ignore'
            and ts.table_schema not in (select schema_name from dataset_labels)
            -- Exclude tables where tables are listed to ignore in ignore_latency_tables_list
            and ts.table_schema || '.' || ts.table_name
            not in (select table_schema || '.' || table_name from table_labels)
    )

select
    '{project_name}' as project_id,
    table_schema as dataset_id,
    table_name as table_id,
    storage_last_modified_time as last_modified_time,
    current_timestamp() as current_time,
    latency_threshold,
    hours_since_last_update,
    source,
    internal_table_name,
    table_pattern
from matched_tables
order by hours_since_last_update desc
;
