-- drop hive table definition if exists
drop table ddbc360view;
-- create hive table definition for the dynamodb TABLE
-- using org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler
-- an mapping columns with "dynamodb.column.mapping" =
CREATE EXTERNAL TABLE ddbc360view
( client_id bigint,
  branch_id string,
  birth_date string,
  age bigint,
  home_ownership string,
  occupation string,
  marital_status string,
  head_of_household_flag string,
  client_created_date string,
  rn bigint,
  b_date string,
  frequency string,
  cr_date string,
  disp_id bigint,
  type string,
  l3mcredit_avg double,
  l3mcredit_count bigint,
  l3m_credit_calc string,
  l3mdebit_avg double,
  l3mdebit_count bigint,
  l3m_debit_calc string,
  l3m_cre_disp_id bigint,
  l3m_deb_disp_id bigint,
  l6mcredit_avg double,
  l6mcredit_count bigint,
  l6m_credit_calc string,
  l6mdebit_avg double,
  l6mdebit_count bigint,
  l6m_debit_calc string,
  l6m_cre_disp_id bigint,
  l6m_deb_disp_id bigint,
  web_hits bigint,
  web_visit_date string,
  web_search1 string,
  web_search2 string,
  web_mobile string
)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES ("dynamodb.table.name" = "DDBc360view",
"dynamodb.column.mapping" = "client_id:pk,branch_id:sk,birth_date:birth_date,age:age,home_ownership:home_ownership,occupation:occupation,marital_status:marital_status,head_of_household_flag:head_of_household_flag,client_created_date:client_created_date,rn:rn,b_date:b_date,frequency:frequency,cr_date:cr_date,disp_id:disp_id,type:type,l3mcredit_avg:l3mcredit_avg,l3mcredit_count:l3mcredit_count,l3m_credit_calc:l3m_credit_calc,l3mdebit_avg:l3mdebit_avg,l3mdebit_count:l3mdebit_count,l3m_debit_calc:l3m_debit_calc,l3m_cre_disp_id:l3m_cre_disp_id,l3m_deb_disp_id:l3m_deb_disp_id,l6mcredit_avg:l6mcredit_avg,l6mcredit_count:l6mcredit_count,l6m_credit_calc:l6m_credit_calc,l6mdebit_avg:l6mdebit_avg,l6mdebit_count:l6mdebit_count,l6m_debit_calc:l6m_debit_calc,l6m_cre_disp_id:l6m_cre_disp_id,l6m_deb_disp_id:l6m_deb_disp_id,web_hits:web_hits,web_visit_date:web_visit_date,web_mobile:web_mobile");
-- insert data from the c360denormalized table to the hive table
insert into ddbc360view
select
client_id,
CASE WHEN branch_id   is NULL THEN '_' ELSE  cast(branch_id as string) end as branch_id,
CASE WHEN birth_date   is NULL THEN '_' ELSE  birth_date end as birth_date,
CASE WHEN age   is NULL THEN 0 ELSE  age  end as age,
CASE WHEN home_ownership  is NULL THEN '_' ELSE  home_ownership end as home_ownership,
CASE WHEN occupation  is NULL THEN '_' ELSE  occupation end as occupation,
CASE WHEN marital_status  is NULL THEN '_' ELSE  marital_status end as marital_status,
CASE WHEN head_of_household_flag  is NULL THEN '_' ELSE  head_of_household_flag end as head_of_household_flag,
CASE WHEN client_created_date  is NULL THEN '_' ELSE  client_created_date end as client_created_date,
CASE WHEN rn  is NULL THEN 0 ELSE  rn end as rn,
CASE WHEN b_date  is NULL THEN '_' ELSE  b_date end as b_date,
CASE WHEN frequency  is NULL THEN '_' ELSE  frequency end as frequency,
CASE WHEN cr_date  is NULL THEN '_' ELSE  cr_date end as cr_date,
CASE WHEN disp_id  is NULL THEN 0 ELSE  disp_id end as disp_id,
CASE WHEN type  is NULL THEN '_' ELSE  type end as type,
CASE WHEN l3mcredit_avg  is NULL THEN 0 ELSE  l3mcredit_avg end as l3mcredit_avg,
CASE WHEN l3mcredit_count  is NULL THEN 0 ELSE  l3mcredit_count end as l3mcredit_count,
CASE WHEN l3m_credit_calc  is NULL THEN '_' ELSE  l3m_credit_calc end as l3m_credit_calc,
CASE WHEN l3mdebit_avg  is NULL THEN 0 ELSE  l3mdebit_avg end as l3mdebit_avg,
CASE WHEN l3mdebit_count  is NULL THEN 0 ELSE  l3mdebit_count end as l3mdebit_count,
CASE WHEN l3m_debit_calc  is NULL THEN '_' ELSE  l3m_debit_calc end as l3m_debit_calc,
CASE WHEN l3m_cre_disp_id  is NULL THEN 0 ELSE  l3m_cre_disp_id end as l3m_cre_disp_id,
CASE WHEN l3m_deb_disp_id  is NULL THEN 0 ELSE  l3m_deb_disp_id end as l3m_deb_disp_id,
CASE WHEN l6mcredit_avg  is NULL THEN 0 ELSE  l6mcredit_avg end as l6mcredit_avg,
CASE WHEN l6mcredit_count  is NULL THEN 0 ELSE  l6mcredit_count end as l6mcredit_count,
CASE WHEN l6m_credit_calc  is NULL THEN '_' ELSE  l6m_credit_calc end as l6m_credit_calc,
CASE WHEN l6mdebit_avg  is NULL THEN 0 ELSE  l6mdebit_avg end as l6mdebit_avg,
CASE WHEN l6mdebit_count  is NULL THEN 0 ELSE  l6mdebit_count end as l6mdebit_count,
CASE WHEN l6m_debit_calc  is NULL THEN '_' ELSE  l6m_debit_calc end as l6m_debit_calc,
CASE WHEN l6m_cre_disp_id  is NULL THEN 0 ELSE  l6m_cre_disp_id end as l6m_cre_disp_id,
CASE WHEN l6m_deb_disp_id  is NULL THEN 0 ELSE  l6m_deb_disp_id end as l6m_deb_disp_id,
CASE WHEN web_hits  is NULL THEN 0 ELSE  web_hits end as web_hits,
CASE WHEN web_visit_date  is NULL THEN '_' ELSE  web_visit_date end as web_visit_date,
CASE WHEN web_search1  is NULL THEN '_' ELSE  web_search1 end as web_search1,
CASE WHEN web_search2  is NULL THEN '_' ELSE  web_search2 end as web_search2,
CASE WHEN web_mobile  is NULL THEN '_' ELSE  cast(web_mobile as string) end as web_mobile
FROM c360view_analytics.c360denormalized
where client_id is not null
and age is not null
limit 100;
