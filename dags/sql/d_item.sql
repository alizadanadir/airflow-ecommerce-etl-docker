insert into warehouse.d_item (item_id, item_name)
select item_id, item_name from staging.user_order_log
where item_id not in (select item_id from warehouse.d_item)
group by item_id, item_name;