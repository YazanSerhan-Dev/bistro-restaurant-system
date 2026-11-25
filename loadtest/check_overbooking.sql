-- Integrity check after a load-test level (runs on bistro_loadtest).
-- Spec: each reservation needs ONE whole table with enough seats, tables are never shared.
-- For tables sorted by size, every reservation set present at the same moment is seatable
-- iff, for every size k: (#parties with >= k people) <= (#tables with >= k seats).
-- The check is evaluated at every reservation start time (when occupancy can increase).
WITH starts AS (
    SELECT DISTINCT reservation_time AS t
    FROM reservation
    WHERE status IN ('CONFIRMED', 'PENDING', 'ARRIVED')
),
sizes AS (
    SELECT DISTINCT num_of_customers AS k FROM reservation
),
demand AS (
    SELECT s.t, z.k,
           (SELECT COUNT(*) FROM reservation r
             WHERE r.status IN ('CONFIRMED', 'PENDING', 'ARRIVED')
               AND r.reservation_time <= s.t AND r.expiry_time > s.t
               AND r.num_of_customers >= z.k) AS parties,
           (SELECT COUNT(*) FROM restaurant_table tb WHERE tb.num_of_seats >= z.k) AS tables
    FROM starts s CROSS JOIN sizes z
)
SELECT
    (SELECT COUNT(*) FROM reservation WHERE status = 'CONFIRMED') AS confirmed_reservations,
    (SELECT COUNT(*) FROM starts)                                  AS time_slots_checked,
    (SELECT COUNT(*) FROM demand WHERE parties > tables)           AS overbooking_violations;
