delete from warehouse.f_daily_sales where date_time::Date = '{{ds}}';
insert into warehouse.f_daily_sales (date_time, item_id, customer_id, city_id, quantity, payment_amount)
select date_time, item_id, customer_id, city_id, quantity, payment_amount from staging.user_order_log uol
where uol.date_time::Date = '{{ds}}';