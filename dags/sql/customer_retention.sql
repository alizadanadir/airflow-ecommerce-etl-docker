truncate table warehouse.customer_retention;

insert into warehouse.customer_retention 
with 
main_table as (
select DATE_TRUNC('week', date_time) AS week_startday, 
customer_id, item_id,
COUNT(DISTINCT date_time) AS order_count,
SUM(payment_amount) AS total_revenue,
MAX(CASE WHEN status = 'refunded' THEN 1 ELSE 0 END) is_refund
from staging.user_order_log uol
group by DATE_TRUNC('week', date_time), customer_id, item_id),

new_customers as (
select week_startday, item_id, 
count(customer_id) new_customers_count,
sum(total_revenue) new_customers_revenue
from main_table
where order_count = 1 and is_refund = 0
group by item_id, week_startday),

returning_customers as (
select week_startday, item_id, 
count(customer_id) returning_customers_count,
sum(total_revenue) returning_customers_revenue
from main_table
where order_count > 1 and is_refund = 0
group by item_id, week_startday),

refunding_customers as (
select week_startday, item_id, count(customer_id) refunding_customers_count from main_table
where is_refund = 1
group by item_id, week_startday)

select 
nc.week_startday,
nc.item_id,
coalesce(nc.new_customers_count,0) new_customers_count,
COALESCE(nc.new_customers_revenue,0) new_customers_revenue,
COALESCE(retc.returning_customers_count,0) returning_customers_count,
COALESCE(retc.returning_customers_revenue,0) returning_customers_revenue,
COALESCE(refc.refunding_customers_count, 0) refunding_customers_count

from new_customers nc
left join returning_customers retc on nc.week_startday = retc.week_startday and nc.item_id = retc.item_id
left join refunding_customers refc on nc.week_startday = refc.week_startday and nc.item_id = refc.item_id
order by week_startday asc;
