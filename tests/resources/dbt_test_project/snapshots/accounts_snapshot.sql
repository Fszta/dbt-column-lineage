{% snapshot accounts_snapshot %}

    {{
        config(
          target_schema='snapshots',
          unique_key='account_id',
          strategy='check',
          check_cols=['account_holder', 'country_id'],
        )
    }}

    select
        account_id,
        account_holder,
        country_id
    from {{ ref('stg_accounts') }}

{% endsnapshot %}
