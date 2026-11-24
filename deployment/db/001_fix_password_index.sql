-- ============================================================
-- Bistro DB migration 001
-- Run ONCE on an existing `bistro` database (fresh imports of the
-- dumps in G20_Assignment3_DB / data1 already include this fix).
--
-- Why: the `password` columns had a UNIQUE index, so two users could
-- not share a password, and a failed registration revealed that some
-- other account uses that password. Passwords are now stored as salted
-- PBKDF2 hashes by the server (old plain-text rows are upgraded
-- automatically on the next successful login).
-- ============================================================

USE `bistro`;

ALTER TABLE `subscribers` DROP INDEX `password`;
ALTER TABLE `staff`       DROP INDEX `password`;
