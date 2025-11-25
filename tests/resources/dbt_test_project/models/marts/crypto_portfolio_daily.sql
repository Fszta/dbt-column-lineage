with

int_trade_flow as (
    select * from {{ ref('int_trade_flow') }}
),

trades_by_settlement as (
    select
        *,
        case
            when trade_status = 'EXECUTED' then settlement_date
            else trade_timestamp
        end as effective_date
    from int_trade_flow
    where trade_status in ('EXECUTED', 'REJECTED', 'DECLINED', 'PROCESSING')
),

crypto_portfolio_daily as (
    select
        wallet_id,
        effective_date::date as portfolio_date,

        -- Buy-side metrics
        count(distinct case
            when trade_direction = 'BUY'
            and trade_status = 'EXECUTED'
            then trade_id
            else null
        end) as buy_trade_count,

        sum(case
            when trade_direction = 'BUY'
            and trade_status = 'EXECUTED'
            then trade_volume
            else 0
        end) as buy_volume_total,

        sum(case
            when trade_direction = 'BUY'
            and trade_status = 'EXECUTED'
            then absolute_trade_value
            else 0
        end) as buy_value_total,

        avg(case
            when trade_direction = 'BUY'
            and trade_status = 'EXECUTED'
            then absolute_trade_value
            else null
        end) as buy_avg_trade_size,

        -- Sell-side metrics
        count(distinct case
            when trade_direction = 'SELL'
            and trade_status = 'EXECUTED'
            then trade_id
            else null
        end) as sell_trade_count,

        sum(case
            when trade_direction = 'SELL'
            and trade_status = 'EXECUTED'
            then trade_volume
            else 0
        end) as sell_volume_total,

        sum(case
            when trade_direction = 'SELL'
            and trade_status = 'EXECUTED'
            then absolute_trade_value
            else 0
        end) as sell_value_total,

        avg(case
            when trade_direction = 'SELL'
            and trade_status = 'EXECUTED'
            then absolute_trade_value
            else null
        end) as sell_avg_trade_size,

        -- Execution state metrics (using trade_status directly)
        count(distinct case
            when trade_status = 'EXECUTED'
            then trade_id
            else null
        end) as executed_trade_count,

        sum(case
            when trade_status = 'EXECUTED'
            then absolute_trade_value
            else 0
        end) as executed_value_total,

        count(distinct case
            when trade_status in ('REJECTED', 'DECLINED')
            then trade_id
            else null
        end) as failed_trade_count,

        sum(case
            when trade_status in ('REJECTED', 'DECLINED')
            then absolute_trade_value
            else 0
        end) as failed_value_total,

        count(distinct case
            when trade_status = 'PROCESSING'
            then trade_id
            else null
        end) as pending_trade_count,

        sum(case
            when trade_status = 'PROCESSING'
            then absolute_trade_value
            else 0
        end) as pending_value_total,

        -- Additional trade_status-based metrics
        count(distinct case
            when trade_status = 'EXECUTED'
            and trade_direction = 'BUY'
            then trade_id
            else null
        end) as executed_buy_count,

        sum(case
            when trade_status = 'EXECUTED'
            and trade_direction = 'BUY'
            then absolute_trade_value
            else 0
        end) as executed_buy_value,

        count(distinct case
            when trade_status = 'EXECUTED'
            and trade_direction = 'SELL'
            then trade_id
            else null
        end) as executed_sell_count,

        sum(case
            when trade_status = 'EXECUTED'
            and trade_direction = 'SELL'
            then absolute_trade_value
            else 0
        end) as executed_sell_value,

        count(distinct case
            when trade_status != 'EXECUTED'
            then trade_id
            else null
        end) as non_executed_count,

        sum(case
            when trade_status != 'EXECUTED'
            then absolute_trade_value
            else 0
        end) as non_executed_value,

        -- Trade type metrics
        count(distinct case
            when trade_type = 'SPOT'
            and trade_status = 'EXECUTED'
            then trade_id
            else null
        end) as spot_trade_count,

        sum(case
            when trade_type = 'SPOT'
            and trade_status = 'EXECUTED'
            then absolute_trade_value
            else 0
        end) as spot_trade_value,

        -- Fee metrics
        coalesce(sum(case
            when trade_status = 'EXECUTED'
            then exchange_fee
            else 0
        end), 0) as total_exchange_fees,

        coalesce(sum(case
            when trade_status = 'EXECUTED'
            then network_fee
            else 0
        end), 0) as total_network_fees,

        coalesce(sum(case
            when trade_status = 'EXECUTED'
            then total_fees
            else 0
        end), 0) as total_fees_paid,

        -- Net metrics
        sum(case
            when trade_status = 'EXECUTED'
            then case
                when trade_direction = 'BUY' then absolute_trade_value
                else -absolute_trade_value
            end
            else 0
        end) as net_trade_value,

        buy_trade_count + sell_trade_count as total_trade_count,
        buy_volume_total + sell_volume_total as total_volume,
        buy_value_total + sell_value_total as gross_trade_value,

        -- Success rate metrics (using trade_status directly)
        case
            when (executed_trade_count + failed_trade_count) > 0
            then executed_trade_count::float / (executed_trade_count + failed_trade_count)
            else 0
        end as execution_success_rate,

        case
            when total_trade_count > 0
            then executed_trade_count::float / total_trade_count
            else 0
        end as overall_success_rate,

        -- Additional status-based aggregations
        count(distinct case
            when trade_status = 'EXECUTED'
            and trade_volume > 100
            then trade_id
            else null
        end) as large_executed_count,

        sum(case
            when trade_status = 'EXECUTED'
            and trade_volume > 100
            then absolute_trade_value
            else 0
        end) as large_executed_value,

        count(distinct case
            when trade_status = 'EXECUTED'
            and trade_volume <= 100
            then trade_id
            else null
        end) as small_executed_count,

        sum(case
            when trade_status = 'EXECUTED'
            and trade_volume <= 100
            then absolute_trade_value
            else 0
        end) as small_executed_value,

        -- Timestamp metrics
        min(case
            when trade_status = 'EXECUTED'
            then execution_timestamp
            else null
        end) as first_execution_time,

        max(case
            when trade_status = 'EXECUTED'
            then execution_timestamp
            else null
        end) as last_execution_time,

        now() as computed_at,
        'daily' as aggregation_period

    from trades_by_settlement
    group by 1, 2
)

select * from crypto_portfolio_daily
