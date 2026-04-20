-- Power BI query with SQL comments that break sqlglot lineage extraction
-- https://github.com/datahub-project/datahub/issues/11251
--
-- Power BI encodes newlines as #(lf) in M-language, but even in decoded form
-- the -- comments cause sqlglot to silently drop all subsequent JOINs and
-- WHERE clauses from lineage. GSP handles comment stripping correctly.
--
-- Test with:
--   gsp-datahub-sidecar --sql-file examples/powerbi_comments.sql --db-vendor dbvmssql --dry-run

CREATE VIEW dbo.customer_branches AS
select
upper(cs.customercode) as customercode
, cs.ear2id as ear2id
, db.branch_rollup_name
 , db.subregion1
 , db.subregion2
from nast_truckload_domain.broker.dim_customer cs
--join nast_customer_domain.broker.dim_customer_ear2_single_ownership_history as so on cs.ear2id = so.ear2_id and is_current = true
join nast_customer_domain.broker.dim_customer_ear2_single_ownership_history as so on cs.ear2id = so.ear2_id and is_current = true
join nast_customer_domain.broker.ref_branch db on db.Branch_code = so.branch_code
-- join nast_customer_domain.broker.customer_long_term_value as clv on cs.ear2id = clv.ear2_id
-- join nast_customer_domain.broker.customer_order_load cl on cl.customer_code = cs.customercode and cl.revenue>0 and cl.activity_date>= '2023-01-01'
where cs.customerstatusid = 1 --active
-- and cs.customercode = 'C8817970'
and db.primary_business_line_id in  ('62','73')
-- and cs.ear2id = 199;
