# Database migrations

Run these **once** on an existing `bistro` database (in MySQL Workbench or `mysql -u <user> -p < file.sql`).
Fresh imports of the dumps in `G20_Assignment3/G20_Assignment3_DB` and `data1` already include the schema fix.

| File | Required? | What it does |
|------|-----------|--------------|
| `001_fix_password_index.sql` | **Yes** | Drops the `UNIQUE` index on `subscribers.password` and `staff.password` (two users could not share a password, and a failed registration revealed that another account uses it). |
| `002_fix_performance_report_dates.sql` | Optional | Re-dates old `performance_log` rows to the day of the visit, so past months show correctly in the manager's performance report. |

## Passwords

The server now stores passwords as salted PBKDF2 hashes. Existing plain-text passwords keep working:
each one is replaced by a hash automatically the next time that user logs in successfully.
