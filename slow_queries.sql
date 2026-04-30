SELECT DATE(last_login) as login_date, country_code, COUNT(*) as dau
FROM users
WHERE last_login >= NOW() - INTERVAL '30 days'
GROUP BY DATE(last_login), country_code
ORDER BY login_date DESC, dau DESC;

-- Q2: Open positions PnL dashboard. Runs every 30s, takes 12s.
SELECT u.email, a.currency, SUM(p.pnl) as total_pnl, COUNT(p.id) as position_count
FROM positions p
JOIN accounts a ON p.account_id = a.id
JOIN users u ON a.user_id = u.id
WHERE p.status = 'open'
GROUP BY u.email, a.currency
ORDER BY total_pnl DESC;

-- Q3: Withdrawal reconciliation. Runs nightly, takes 8 minutes.
SELECT t.reference, t.amount, t.currency, t.created_at, t.processed_at,
       u.email, u.country_code,
       EXTRACT(EPOCH FROM (t.processed_at - t.created_at))/3600 as processing_hours
FROM transactions t
JOIN accounts a ON t.account_id = a.id
JOIN users u ON a.user_id = u.id
WHERE t.transaction_type = 'withdrawal'
AND t.status = 'completed'
AND t.created_at >= NOW() - INTERVAL '90 days'
ORDER BY t.created_at DESC;

-- Q4: KYC compliance report. Runs weekly, takes 3 minutes.
SELECT u.country_code,
       COUNT(*) FILTER (WHERE u.kyc_status = 'verified') as verified,
       COUNT(*) FILTER (WHERE u.kyc_status = 'pending') as pending,
       COUNT(*) FILTER (WHERE u.kyc_status = 'rejected') as rejected,
       AVG(EXTRACT(EPOCH FROM (u.last_login - u.created_at))/86400) as avg_days_to_first_login
FROM users u
WHERE u.created_at >= '2024-01-01'
GROUP BY u.country_code
ORDER BY verified DESC;

-- Q5: Audit trail search. Ad hoc, users report it times out.
SELECT al.created_at, al.action, al.ip_address, al.metadata,
       u.email, u.country_code
FROM audit_log al
JOIN users u ON al.user_id = u.id
WHERE al.metadata @> '{"account_id": 12345}'
AND al.created_at >= NOW() - INTERVAL '7 days'
ORDER BY al.created_at DESC;