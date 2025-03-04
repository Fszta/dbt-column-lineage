from src.registry.sql_parser import SQLColumnParser
from src.models.schema import ColumnLineage

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
    
    assert "customer_id" in lineage
    assert "name" in lineage
    assert "amount" in lineage
    
    # Check renamed column
    customer_id_lineage = lineage["customer_id"][0]
    assert customer_id_lineage.transformation_type == "renamed"
    assert customer_id_lineage.source_columns == {"customers.id"}
    
    # Check direct reference
    name_lineage = lineage["name"][0]
    assert name_lineage.transformation_type == "direct"
    assert name_lineage.source_columns == {"customers.name"}

def test_cte_with_aggregation_and_aliases():
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
    
    # Check direct references
    assert lineage["id"][0].transformation_type == "direct"
    assert lineage["id"][0].source_columns == {"customers.id"}
    
    # Check aggregations
    order_count = lineage["order_count"][0]
    assert order_count.transformation_type == "derived"
    assert "count" in order_count.sql_expression.lower()
    assert order_count.source_columns == {"orders.id"}
    
    total_amount = lineage["total_amount"][0]
    assert total_amount.transformation_type == "derived"
    assert "sum" in total_amount.sql_expression.lower()
    assert total_amount.source_columns == {"orders.amount"}
    
    # Check complex transformation
    customer_tier = lineage["customer_tier"][0]
    assert customer_tier.transformation_type == "derived"
    assert "case" in customer_tier.sql_expression.lower()
    assert customer_tier.source_columns == {"orders.amount"}

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
    
    # Check derived columns from first CTE
    assert "order_count" in lineage
    order_count = lineage["order_count"][0]
    assert order_count.transformation_type == "derived"
    assert "count" in order_count.sql_expression.lower()
    assert order_count.source_columns == {"orders.id"}
    
    assert "total_amount" in lineage
    total_amount = lineage["total_amount"][0]
    assert total_amount.transformation_type == "derived"
    assert "sum" in total_amount.sql_expression.lower()
    assert total_amount.source_columns == {"orders.amount"}
    
    # Check derived column from second CTE
    assert "customer_tier" in lineage
    customer_tier = lineage["customer_tier"][0]
    assert customer_tier.transformation_type == "derived"
    assert "case" in customer_tier.sql_expression.lower()
    assert "total_amount" in customer_tier.sql_expression
    assert customer_tier.source_columns == {"orders.amount"}
    
    # Check that columns are preserved through the * selections
    assert "customer_id" in lineage
    customer_id = lineage["customer_id"][0]
    assert customer_id.source_columns == {"orders.customer_id"}
    
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
    
    # Check the original aggregation
    total_revenue = lineage["total_revenue"][0]
    #assert total_revenue.transformation_type == "derived"
    assert total_revenue.source_columns == {"orders.amount"}
    
    # Check derived calculations
    revenue_hundreds = lineage["revenue_hundreds"][0]
    #assert revenue_hundreds.transformation_type == "derived"
    assert revenue_hundreds.source_columns == {"orders.amount"}
    
    # Check complex case statement
    revenue_tier = lineage["revenue_tier"][0]
    #assert revenue_tier.transformation_type == "derived"
    assert revenue_tier.source_columns == {"orders.amount"}

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
    
    # Check window aggregation
    customer_total = lineage["customer_total"][0]
    assert customer_total.transformation_type == "derived"
    assert customer_total.source_columns == {"orders.amount", "orders.customer_id"}
    
    # Check ranking
    amount_rank = lineage["amount_rank"][0]
    assert amount_rank.transformation_type == "derived"
    assert amount_rank.source_columns == {"orders.amount", "orders.customer_id"}
    
    # Check window calculation
    amount_pct = lineage["amount_pct"][0]
    assert amount_pct.transformation_type == "derived"
    assert amount_pct.source_columns == {"orders.amount", "orders.customer_id"}

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
    
    # Check scalar subquery
    order_count = lineage["order_count"][0]
    assert order_count.transformation_type == "derived"
    assert "orders.customer_id" in order_count.source_columns
    assert "customers.id" in order_count.source_columns
    
    # Check exists subquery
    has_large_orders = lineage["has_large_orders"][0]
    assert has_large_orders.transformation_type == "derived"
    assert "orders.amount" in has_large_orders.source_columns
    assert "orders.customer_id" in has_large_orders.source_columns
    assert "customers.id" in has_large_orders.source_columns
    