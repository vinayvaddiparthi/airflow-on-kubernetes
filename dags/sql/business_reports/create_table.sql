create table if not exists {{ params.table_name }} (
    lookup_key varchar primary key,
    raw_response varchar,
    last_modified_at timestamp,
    batch_import_timestamp timestamp
);