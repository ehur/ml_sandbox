select parent_category_category_code,child_category_category_code,advertiser_cid,sum(sum_comm_amount_usd)
from cape_tapcw_aggr_month c,dim_website d
where c.dim_website = d.dimension_key
and month >= 201401 and month <= 201506
and parent_category_category_code > 0
group by parent_category_category_code,child_category_category_code,advertiser_cid
having sum(sum_comm_amount_usd) > 1000
order by  parent_category_category_code,child_category_category_code;
