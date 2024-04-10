delete from warehouse.f_daily_activity where date_time::Date  = '{{ds}}';
insert into warehouse.f_daily_activity
select date_time,action_id,customer_id,quantity from staging.user_activity_log ual 
where date_time  = '{{ ds }}' 
group by date_time,action_id,customer_id,quantity;