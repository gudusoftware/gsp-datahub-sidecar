-- Oracle CREATE VIEW with subqueries, aggregation, and cross-join
--
-- Test with:
--   gsp-datahub-sidecar --sql-file examples/oracle_create_view.sql --db-vendor dbvoracle --dry-run

CREATE VIEW vsal
AS
  SELECT a.deptno                  "Department",
         a.num_emp / b.total_count "Employees",
         a.sal_sum / b.total_sal   "Salary"
  FROM   (SELECT deptno,
                 Count()  num_emp,
                 SUM(sal) sal_sum
          FROM   scott.emp
          WHERE  city = 'NYC'
          GROUP  BY deptno) a,
         (SELECT Count()  total_count,
                 SUM(sal) total_sal
          FROM   scott.emp
          WHERE  city = 'NYC') b
;
