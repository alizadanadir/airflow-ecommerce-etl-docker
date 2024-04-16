CREATE SCHEMA staging; 
CREATE SCHEMA warehouse;

CREATE TABLE staging.user_order_log (
	uniq_id varchar(32) NOT NULL,
	date_time timestamp NOT NULL,
	city_id int4 NOT NULL,
	city_name varchar(100) NULL,
	customer_id int4 NOT NULL,
	first_name varchar(100) NULL,
	last_name varchar(100) NULL,
	item_id int4 NOT NULL,
	item_name varchar(100) NULL,
	quantity int8 NULL,
	payment_amount numeric(10, 2) NULL,
	status varchar(32) NOT NULL,
	CONSTRAINT user_order_log_pk PRIMARY KEY (uniq_id)
);
CREATE INDEX uo1 ON staging.user_order_log USING btree (customer_id);
CREATE INDEX uo2 ON staging.user_order_log USING btree (item_id);

CREATE TABLE staging.customer_research(
    date_id	timestamp,
    category_id	int4,
    geo_id	int4,
    sales_qty	int4,
    sales_amt numeric(10, 2)
) ;

CREATE TABLE staging.user_activity_log(
	uniq_id varchar(32) NOT NULL,    
    date_time	timestamp,
    action_id int4,
    customer_id	int4,
    quantity int8
) ;

--
CREATE TABLE warehouse.d_city (
	id serial4 NOT NULL,
	city_id int4 NULL,
	city_name varchar(50) NULL,
	CONSTRAINT d_city_city_id_key UNIQUE (city_id),
	CONSTRAINT d_city_pkey PRIMARY KEY (id)
);
CREATE INDEX d_city1 ON warehouse.d_city USING btree (city_id);

CREATE TABLE warehouse.d_customer (
	id serial4 NOT NULL,
	customer_id int4 NOT NULL,
	first_name varchar(15) NULL,
	last_name varchar(15) NULL,
	city_id int4 NULL,
	CONSTRAINT d_customer_customer_id_key UNIQUE (customer_id),
	CONSTRAINT d_customer_pkey PRIMARY KEY (id)
);
CREATE INDEX d_cust1 ON warehouse.d_customer USING btree (customer_id);

CREATE TABLE warehouse.d_item (
	id serial4 NOT NULL,
	item_id int4 NOT NULL,
	item_name varchar(50) NULL,
	CONSTRAINT d_item_item_id_key UNIQUE (item_id),
	CONSTRAINT d_item_pkey PRIMARY KEY (id)
);
CREATE UNIQUE INDEX d_item1 ON warehouse.d_item USING btree (item_id);

CREATE TABLE warehouse.f_research (
	date_time timestamp NULL,
	category_id int4 NULL,
	geo_id int4 NULL,
	sales_qty int4 NULL,
	sales_amt numeric NULL
);

CREATE TABLE warehouse.f_daily_sales (
	id serial4 NOT NULL,
	date_time timestamp NOT NULL,
	item_id int4 NOT NULL,
	customer_id int4 NOT NULL,
	city_id int4 NOT NULL,
	quantity int8 NULL,
	payment_amount numeric(10, 2) NULL,
	CONSTRAINT f_daily_sales_pkey PRIMARY KEY (id)
);
CREATE INDEX f_ds1 ON warehouse.f_daily_sales USING btree (date_id);
CREATE INDEX f_ds2 ON warehouse.f_daily_sales USING btree (item_id);
CREATE INDEX f_ds3 ON warehouse.f_daily_sales USING btree (customer_id);
CREATE INDEX f_ds4 ON warehouse.f_daily_sales USING btree (city_id);

ALTER TABLE warehouse.f_daily_sales ADD CONSTRAINT f_daily_sales_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES warehouse.d_customer(customer_id);
ALTER TABLE warehouse.f_daily_sales ADD CONSTRAINT f_daily_sales_item_id_fkey FOREIGN KEY (item_id) REFERENCES warehouse.d_item(item_id);
ALTER TABLE warehouse.f_daily_sales ADD CONSTRAINT f_daily_sales_item_id_fkey1 FOREIGN KEY (item_id) REFERENCES warehouse.d_item(item_id);

CREATE TABLE warehouse.f_daily_activity (
	date_time timestamp NULL,
	action_id int4 NULL,
	customer_id int4 NULL,
	quantity int8 NULL
);

-- datamart table

CREATE TABLE warehouse.customer_retention (
	week_startday timestamp NULL,
	item_id int4 NULL,
	new_customers_count int8 NULL,
	new_customers_revenue numeric NULL,
	returning_customers_count int8 NULL,
	returning_customers_revenue numeric NULL,
	refunding_customers_count int8 NULL
);

CREATE TABLE staging.dq_checks_results (
    Table_name VARCHAR(255),
    DQ_check_name VARCHAR(255),
    Datetime TIMESTAMP,
    DQ_check_result NUMERIC(8,2) CHECK (DQ_check_result IN (0, 1))
);
