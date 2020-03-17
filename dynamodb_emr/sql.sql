with
w as (SELECT *,
rank() OVER (PARTITION BY client_id
                    ORDER BY branch_id,frequency DESC) AS rnk
               FROM c360view_analytics.c360denormalized
order by client_id)
select * from w where rnk=1


select distinct client_id,branch_id,frequency
from c360view_analytics.c360denormalized
order by client_id,frequency



with
t as (SELECT c.*,
a.branch_id, a.frequency,a.cr_date,
gb.disp_id, gb.type,
l3mc.amount_avg as l3mcredit_avg, l3mc.amount_count as l3mcredit_count, l3mc.calc_date||' '||l3mc.calc_time as l3m_credit_calc,
l3md.amount_avg as l3mdebit_avg, l3mc.amount_count as l3mdebit_count, l3md.calc_date||' '||l3md.calc_time as l3m_debit_calc,
l3mc.disp_id as l3m_cre_disp_id,
l3md.disp_id as l3m_deb_disp_id,
l6mc.amount_avg as l6mcredit_avg, l6mc.amount_count as l6mcredit_count, l6mc.calc_date||' '||l6mc.calc_time as l6m_credit_calc,
l6md.amount_avg as l6mdebit_avg, l6mc.amount_count as l6mdebit_count, l6md.calc_date||' '||l6md.calc_time as l6m_debit_calc,
l6mc.disp_id as l6m_cre_disp_id,
l6md.disp_id as l6m_deb_disp_id,
ga.hits as web_hits,
ga.lastdate as web_visit_date,
ga.search1 as web_search1,
ga.search2 as web_search2,
ga.mobile as web_mobile
FROM c360view_stage.gbank_pqt as gb
left outer join c360view_stage.account_pqt as a on gb.account_id = a.account_id
left outer join c360view_stage.customer_pqt as c on gb.client_id = c.client_id
left outer join c360view_stage.n1_c360_trans_stats_type_l3m as l3mc on gb.client_id = l3mc.client_id and l3mc.type='credit'
left outer join c360view_stage.n1_c360_trans_stats_type_l3m as l3md on gb.client_id = l3md.client_id and l3md.type='debit'
left outer join c360view_stage.n1_c360_trans_stats_type_l6m as l6mc on gb.client_id = l6mc.client_id and l6mc.type='credit'
left outer join c360view_stage.n1_c360_trans_stats_type_l6m as l6md on gb.client_id = l6md.client_id and l6md.type='debit'
left outer join (SELECT client_id, sum(t_hits) hits,max(visitdate) lastdate,max(hitpage_pagepathlevel2) search1,max(hitpage_pagepathlevel3) search2,max(device_ismobile) mobile FROM c360view_analytics.ga_sessions_analytics group by client_id) as ga on gb.client_id = ga.client_id ),
r as (SELECT *,
rank() OVER (PARTITION BY client_id
                    ORDER BY branch_id,frequency DESC) AS rnk
               FROM t
order by client_id)
select * from r where rnk=1
"



key=['library/awswrangler-0.0.12-glue-none-any.whl',
'library/n1_c360_dispositions.py',
'library/cust360_etl_mf_trans.py',
'data/visitors/ga_visitors.csv']
