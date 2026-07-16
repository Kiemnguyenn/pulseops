SELECT
    DATE(transaction_time) AS txn_date,
    status,
    COUNT(*) AS txn_count,
    SUM(amount) AS total_amount
FROM {{ ref('stg_transactions') }}
GROUP BY 1, 2
