with

source as (
    select * from {{ source('raw', 'transactions') }}
),

renamed as (
    select
        cast(id as integer) as trade_id,
        cast(account_id as integer) as wallet_id,
        cast(amount as float) as trade_amount,
        status as trade_status,
        cast(transaction_date as date) as trade_timestamp
    from source
),

enriched as (
    select
        trade_id,
        wallet_id,
        trade_amount,
        trade_status,
        trade_timestamp,
        case
            when trade_amount > 0 then 'BUY'
            else 'SELL'
        end as trade_direction,
        abs(trade_amount) as trade_volume,
        case
            when trade_status = 'EXECUTED' then trade_timestamp
            else null
        end as execution_timestamp
    from renamed
    where trade_status in ('EXECUTED', 'REJECTED', 'DECLINED', 'PROCESSING')
)

select * from enriched
