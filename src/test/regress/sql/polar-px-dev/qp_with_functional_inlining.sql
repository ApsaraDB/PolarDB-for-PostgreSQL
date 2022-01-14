-- See qp_with_functional.sql
--
-- Launch the tests with CTE sharing on (with planner), or with CTE inlining
-- on (with ORCA).
--set gp_cte_sharing = on;
set polar_px_optimizer_cte_inlining = on;
set polar_px_optimizer_cte_inlining_bound=1000;

create schema qp_with_functional_inlining;
set search_path='qp_with_functional_inlining';
\i sql/polar-px-dev/qp_with_functional.sql
