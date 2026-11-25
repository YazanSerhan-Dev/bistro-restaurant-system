-- Seed data for the load test (runs on `bistro_loadtest` only, after the dumps are imported).

-- Start every run from the same clean floor: all tables free, no holds.
UPDATE restaurant_table
SET status = 'FREE', reserved_for_reservation_id = NULL, reserved_for_waiting_id = NULL, reserved_until = NULL;

-- Nothing "in progress" from the sample data (it would trigger background jobs differently per run).
UPDATE reservation  SET status = 'EXPIRED'  WHERE status IN ('CONFIRMED', 'PENDING', 'ARRIVED');
UPDATE waiting_list SET status = 'EXPIRED'  WHERE status IN ('WAITING', 'ASSIGNED', 'ARRIVED');
UPDATE visit        SET actual_end_time = actual_start_time WHERE actual_end_time IS NULL;

-- Subscribers used by the virtual users (plain-text password: the server upgrades it to a
-- PBKDF2 hash on the first successful login, like any old account).
INSERT INTO subscribers (username, password, name, phone, email, member_code, barcode_data, birth_date)
SELECT CONCAT('load', n), 'LoadPass1!', CONCAT('Load User ', n),
       CONCAT('058', LPAD(n, 7, '0')), CONCAT('load', n, '@loadtest.local'),
       CONCAT('LOADMEM', n), CONCAT('LOADBAR', n), '1995-01-01'
FROM (SELECT 1 n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
      UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10) x;
