with recent_pubs as
(
select company.id as pub_id
from company, affiliate
where company.id = affiliate.company
and company.status = 0
and company.date_ >= sysdate -10
and affiliate.hidden = 0
)
select website.company,website.id, web_Cat_link.category
from website, web_cat_link, recent_pubs
where website.id = web_cat_link.website
and website.company = recent_pubs.pub_id
and significance=1
order by website.company,website.id;