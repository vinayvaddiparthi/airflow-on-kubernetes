create table if not exists {{ params.table_name }} (
    lookup_key varchar primary key,
    raw_response variant,
    last_modified_at timestamp_ntz,
    batch_import_timestamp timestamp_ntz
);