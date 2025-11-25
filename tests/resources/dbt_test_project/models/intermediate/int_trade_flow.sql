with

stg_crypto_trades as (
    select * from {{ ref('stg_crypto_trades') }}
),

int_trade_flow as (
    select
        trade_id,
        wallet_id,
        trade_amount,
        trade_status,
        trade_timestamp,
        trade_direction,
        trade_volume,
        execution_timestamp,
        case
            when trade_amount > 0 then trade_amount
            else -trade_amount
        end as absolute_trade_value,
        case
            when trade_status = 'EXECUTED' then 'EXECUTED'
            when trade_status = 'REJECTED' then 'FAILED'
            when trade_status = 'DECLINED' then 'FAILED'
            when trade_status = 'PROCESSING' then 'PENDING'
            else 'PENDING'
        end as execution_state,
        case
            when trade_status = 'EXECUTED' then trade_timestamp
            else null
        end as settlement_date,
        'SPOT' as trade_type,
        null as exchange_fee,
        null as network_fee,
        0.0 as total_fees
    from stg_crypto_trades
    where trade_status in ('EXECUTED', 'REJECTED', 'DECLINED', 'PROCESSING')
)

select * from int_trade_flow
