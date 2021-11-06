create table if not exists {{ params.table_name }} (
    lookup_key varchar(50) primary key,
    raw_response variant,
    batch_timestamp timestamp
);