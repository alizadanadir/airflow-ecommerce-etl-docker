select case when count(*) = 0 then 1 else null end result from staging.user_order_log where customer_id is null