from dbt_column_lineage.parser.sql_parser import SQLColumnParser

def test_simple_select_with_join():
    """Test parsing a simple SELECT statement with direct references and renames."""
    sql = """
    select 
        customers.id as customer_id,
        customers.name,
        orders.amount
    from customers
    join orders on orders.customer_id = customers.id
    """
    
    parser = SQLColumnParser()
    lineage = parser.parse_column_lineage(sql)
    
    assert set(lineage.keys()) == {"customer_id", "name", "amount"}
    assert lineage["customer_id"][0].transformation_type == "renamed"
    assert lineage["customer_id"][0].source_columns == {"customers.id"}
    assert lineage["name"][0].transformation_type == "direct"
    assert lineage["name"][0].source_columns == {"customers.name"}
    assert lineage["amount"][0].transformation_type == "direct"
    assert lineage["amount"][0].source_columns == {"orders.amount"}

def test_cte_with_aggregation_and_table_aliases():
    """Test parsing a query with CTEs and aggregations."""
    sql = """
    with customer_orders as (
        select 
            customer_id,
            count(orders.id) as order_count,
            sum(orders.amount) as total_amount
        from orders
        group by customer_id
    )
    select 
        c.id,
        c.name,
        co.order_count,
        co.total_amount,
        case 
            when co.total_amount > 1000 then 'high'
            else 'low'
        end as customer_tier
    from customers c
    left join customer_orders co on co.customer_id = c.id
    """
    
    parser = SQLColumnParser()
    lineage = parser.parse_column_lineage(sql)
    
    assert lineage["id"][0].source_columns == {"customers.id"}
    assert lineage["name"][0].source_columns == {"customers.name"}
    assert lineage["order_count"][0].transformation_type == "derived"
    assert "count" in lineage["order_count"][0].sql_expression.lower()
    assert lineage["order_count"][0].source_columns == {"orders.id"}
    assert lineage["total_amount"][0].transformation_type == "derived"
    assert "sum" in lineage["total_amount"][0].sql_expression.lower()
    assert lineage["total_amount"][0].source_columns == {"orders.amount"}
    assert lineage["customer_tier"][0].transformation_type == "derived"
    assert "case" in lineage["customer_tier"][0].sql_expression.lower()
    assert lineage["customer_tier"][0].source_columns == {"orders.amount"}

def test_multiple_ctes():
    """Test parsing a query with multiple CTEs and wildcard select."""
    sql = """
    with orders as (
        select * from orders
    ),
    customers as (
        select * from customers
    ),
    customer_orders as (
        select 
            customer_id,
            count(id) as order_count,
            sum(amount) as total_amount
        from orders
        group by customer_id
    ),
    customer_orders_with_tiering as (
        select 
            *,
            case 
                when total_amount > 1000 then 'tier_1'
                else 'tier_2'
            end as customer_tier
        from customer_orders
        left join customers on customers.id = customer_id
    ),
    final as (
        select * from customer_orders_with_tiering
    )
    select * from final
    """
    
    parser = SQLColumnParser()
    lineage = parser.parse_column_lineage(sql)
    
    assert lineage["order_count"][0].transformation_type == "derived"
    assert lineage["order_count"][0].source_columns == {"orders.id"}
    assert lineage["total_amount"][0].transformation_type == "derived"
    assert lineage["total_amount"][0].source_columns == {"orders.amount"}
    assert lineage["customer_tier"][0].transformation_type == "derived"
    assert "total_amount" in lineage["customer_tier"][0].sql_expression
    assert lineage["customer_tier"][0].source_columns == {"orders.amount"}
    assert lineage["customer_id"][0].source_columns == {"orders.customer_id"}

def test_nested_ctes_with_transformations():
    """Test nested CTEs with multiple transformations of the same column."""
    sql = """
    with revenue as (
        select 
            customer_id,
            sum(amount) as total_revenue
        from orders
        group by customer_id
    ),
    revenue_tiers as (
        select
            customer_id,
            total_revenue,
            case 
                when total_revenue > 1000 then 'high'
                when total_revenue > 500 then 'medium'
                else 'low'
            end as revenue_tier,
            total_revenue / 100 as revenue_hundreds
        from revenue
    )
    select * from revenue_tiers
    """
    
    parser = SQLColumnParser()
    lineage = parser.parse_column_lineage(sql)
    
    assert lineage["total_revenue"][0].source_columns == {"orders.amount"}
    assert lineage["revenue_hundreds"][0].source_columns == {"orders.amount"}
    assert lineage["revenue_tier"][0].source_columns == {"orders.amount"}

def test_window_functions():
    """Test parsing window functions and partitions."""
    sql = """
    select
        customer_id,
        amount,
        sum(amount) over (partition by customer_id) as customer_total,
        rank() over (partition by customer_id order by amount desc) as amount_rank,
        amount / sum(amount) over (partition by customer_id) as amount_pct
    from orders
    """
    
    parser = SQLColumnParser()
    lineage = parser.parse_column_lineage(sql)
    
    assert lineage["customer_total"][0].transformation_type == "derived"
    assert lineage["customer_total"][0].source_columns == {"orders.amount", "orders.customer_id"}
    assert lineage["amount_rank"][0].transformation_type == "derived"
    assert lineage["amount_rank"][0].source_columns == {"orders.amount", "orders.customer_id"}
    assert lineage["amount_pct"][0].transformation_type == "derived"
    assert lineage["amount_pct"][0].source_columns == {"orders.amount", "orders.customer_id"}

def test_subqueries():
    """Test parsing subqueries in different contexts."""
    sql = """
    select 
        customers.id,
        customers.name,
        (select count(*) from orders where orders.customer_id = customers.id) as order_count,
        exists(
            select 1 
            from orders 
            where orders.customer_id = customers.id 
            and orders.amount > 1000
        ) as has_large_orders
    from customers
    """
    
    parser = SQLColumnParser()
    lineage = parser.parse_column_lineage(sql)
    
    assert lineage["order_count"][0].transformation_type == "derived"
    assert "orders.customer_id" in lineage["order_count"][0].source_columns
    assert "customers.id" in lineage["order_count"][0].source_columns
    assert lineage["has_large_orders"][0].transformation_type == "derived"
    assert "orders.amount" in lineage["has_large_orders"][0].source_columns
    assert "orders.customer_id" in lineage["has_large_orders"][0].source_columns
    assert "customers.id" in lineage["has_large_orders"][0].source_columns
    