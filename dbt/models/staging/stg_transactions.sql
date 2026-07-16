SELECT
    transaction_id,
    transaction_time,
    amount,
    sender_account_id,
    receiver_account_id,
    transaction_type,
    status,
    channel
FROM {{ source('bronze', 'bronze_transactions') }}
