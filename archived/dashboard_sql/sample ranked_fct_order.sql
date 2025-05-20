create table bi_global_pricing_dev.ranked_fct_order as

select source_id, date_trunc('year', order_date), count(*)
	from (select source_id, order_date, (random() * 100)::int as r
		from dwh_il.ranked_fct_order)
where r < 10 -- return around 10% of the dataset
and order_date > current_date - 7
group by 1,2
order by 1, 2 desc
limit 100000;

grant all on bi_global_pricing_dev.ranked_fct_order to group bi_global_pricing;
grant all on bi_global_pricing_dev.ranked_fct_order to group bi_global;
grant all on bi_global_pricing_dev.ranked_fct_order to group bi_foodora;
grant all on bi_global_pricing_dev.ranked_fct_order to tableau_global;