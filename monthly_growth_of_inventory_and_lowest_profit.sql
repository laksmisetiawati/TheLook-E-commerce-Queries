WITH calendar AS (
  -- Generate a list of all months in the year 2022
  SELECT 1 AS month UNION ALL
  SELECT 2 UNION ALL
  SELECT 3 UNION ALL
  SELECT 4 UNION ALL
  SELECT 5 UNION ALL
  SELECT 6 UNION ALL
  SELECT 7 UNION ALL
  SELECT 8 UNION ALL
  SELECT 9 UNION ALL
  SELECT 10 UNION ALL
  SELECT 11 UNION ALL
  SELECT 12
), 
product_categories AS (
  -- Get distinct product categories
  SELECT DISTINCT product_category
  FROM `bigquery-public-data.thelook_ecommerce.inventory_items`
), 
calendar_product_categories AS (
  -- Create a Cartesian product of all months and product categories
  SELECT 
    calendar.month, 
    product_categories.product_category AS product_category
  FROM calendar
  CROSS JOIN product_categories
), 
orders AS (
  -- Your original query to get monthly sales data
  SELECT 
    EXTRACT(MONTH FROM order_items.created_at) AS month,
    inventory_items.product_category AS product_category,
    COUNT(DISTINCT order_items.order_id) AS total_order,
    SUM(order_items.sale_price) AS sale_price,
    SUM(inventory_items.cost) AS cost
  FROM `bigquery-public-data.thelook_ecommerce.order_items` AS order_items
  JOIN `bigquery-public-data.thelook_ecommerce.inventory_items` AS inventory_items
    ON order_items.product_id = inventory_items.product_id
  WHERE 
    order_items.status = 'Complete'
    AND EXTRACT(YEAR FROM order_items.created_at) = 2022
  GROUP BY 
    product_category,
    month
),
impute_empty AS (
  SELECT 
    calendar_product_categories.month,
    calendar_product_categories.product_category,
    COALESCE(orders.total_order, 0) AS total_order,
    COALESCE(orders.sale_price, 0) AS sale_price,
    COALESCE(orders.cost, 0) AS cost
  FROM calendar_product_categories
  LEFT JOIN orders
    ON calendar_product_categories.month = orders.month
    AND calendar_product_categories.product_category = orders.product_category
),
monthly_revenue_cost AS (
  /**
    Aggregate monthly total revenue and 
    total cost per product category
  */
  SELECT *,
    sale_price * total_order AS total_revenue,
    cost * total_order AS total_cost
  FROM impute_empty
),
monthly_inventory AS (
  /** Get monthly inventory growth per product categories */
  SELECT 
    product_category AS product_categories,
    EXTRACT(MONTH FROM created_at) AS month,
    COUNT(id) AS total_inventory,
  FROM `bigquery-public-data.thelook_ecommerce.inventory_items`
  WHERE 
    EXTRACT(YEAR FROM created_at)=2022
  GROUP BY 
    product_categories,
    month
),
prev_monthly_inventory AS (
  /**
    Get previous monthly inventory growth 
    per product categories
  */
  SELECT 
    monthly_inventory.product_categories,
    monthly_inventory.month,
    monthly_revenue_cost.total_revenue,
    monthly_revenue_cost.total_cost,
    monthly_inventory.total_inventory,
    LAG(total_inventory) 
      OVER(PARTITION BY monthly_inventory.product_categories ORDER BY monthly_inventory.month) 
      AS prev_inventory,
    monthly_revenue_cost.total_order AS total_order,
    LAG(total_order) 
      OVER(PARTITION BY monthly_revenue_cost.product_category ORDER BY monthly_revenue_cost.month) 
      AS prev_total_order
  FROM monthly_inventory
  JOIN monthly_revenue_cost
    ON monthly_revenue_cost.product_category = monthly_inventory.product_categories
    AND monthly_revenue_cost.month = monthly_inventory.month
),
agg_monthly_inventory AS (
  /**
    Aggregate previous monthly inventory growth 
    per product categories
  */
  SELECT
    product_categories,
    month,
    total_order,
    total_inventory AS in_inventory,
    prev_inventory,
    CASE 
      WHEN month=1 THEN
        total_inventory - total_order
      ELSE
        (prev_inventory + total_inventory) - total_order
    END AS inventory_after_sale,
    total_revenue,
    total_cost
  FROM prev_monthly_inventory
),
inventory_growth AS (
  SELECT agg_monthly_inventory.*,
    CONCAT(
      ROUND(
        (
            (SUM(inventory_after_sale) - SUM(total_order)) 
            / SUM(inventory_after_sale)
        ) * 100, 2
      ), '%'
    ) AS inventory_growth,
  FROM agg_monthly_inventory
  GROUP BY product_categories, month, total_order, 
    in_inventory, prev_inventory, inventory_after_sale, total_revenue, total_cost
),
agg_profit AS (
  SELECT inventory_growth.*,
    total_revenue - total_cost AS profit,
  FROM inventory_growth
  GROUP BY product_categories, month, total_order, 
    in_inventory, prev_inventory, inventory_after_sale, 
    total_revenue, total_cost, inventory_growth
),
total_revenue_profit_per_month AS (
  SELECT
    month,
    SUM(total_revenue) AS monthly_total_revenue,
    SUM(profit) AS monthly_total_profit
  FROM agg_profit
  GROUP BY month
),
profit_distribution AS (
  SELECT agg_profit.*,
    CONCAT(
      ROUND(
        (NULLIF(profit, 0) / monthly_total_profit) * 100, 
        2
      ), 
      '%'
    ) AS profit_distribution
  FROM agg_profit
  JOIN total_revenue_profit_per_month 
    ON total_revenue_profit_per_month.month = agg_profit.month
  GROUP BY product_categories, month, total_order, 
    in_inventory, prev_inventory, inventory_after_sale, 
    total_revenue, total_cost, inventory_growth, profit,
    monthly_total_revenue, monthly_total_profit 
),
revenue_distribution AS (
  SELECT profit_distribution.*,
    CONCAT(
      ROUND(
        (NULLIF(total_revenue, 0) / monthly_total_revenue) * 100, 
        2
      ), 
      '%'
    ) AS revenue_distribution
  FROM profit_distribution
  JOIN total_revenue_profit_per_month 
    ON total_revenue_profit_per_month.month = profit_distribution.month
  GROUP BY product_categories, month, total_order, 
    in_inventory, prev_inventory, inventory_after_sale, 
    total_revenue, total_cost, inventory_growth, profit,
    profit_distribution, monthly_total_revenue 
)
SELECT * FROM revenue_distribution
ORDER BY product_categories, month;
