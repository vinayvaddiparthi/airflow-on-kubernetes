create or replace table {table} (
	id varchar(255),
	file_id varchar(255),
	merchant_guid varchar(255),
	batch_timestamp timestamp_ntz(9),
	batch_balance number(38,2),
	account_guid varchar(255),
	account_category varchar(255),
	account_currency varchar(255),
	transaction_guid varchar(255),
	rel_transaction_id number(38,0),
	credit number(38,2),
	debit number(38,2),
	balance number(38,2),
	date date,
	description varchar(10000),
	predicted_category varchar(255),
	is_nsd boolean
);