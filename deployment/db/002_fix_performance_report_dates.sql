-- ============================================================
-- Bistro DB migration 002 (optional, run once)
--
-- Why: the monthly job used to stamp performance_log.report_date with the
-- day the job ran (e.g. June 1st) instead of the day of the visit, so the
-- manager's performance report showed last month's data under the wrong month.
-- The server now writes the visit day; this fixes rows written before.
-- ============================================================

USE `bistro`;

UPDATE performance_log pl
JOIN visit v ON v.visit_id = pl.visit_id
SET pl.report_date = DATE(v.actual_start_time);
