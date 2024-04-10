delete from warehouse.f_research where date_id::Date  = '{{ds}}';
insert into warehouse.f_research 
select date_id,category_id,geo_id,sales_qty,sum(sales_amt) from staging.customer_research cr
where date_id::Date  = '{{ds}}'
group by date_id,category_id,geo_id,sales_qty;