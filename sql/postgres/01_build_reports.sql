DROP TABLE IF EXISTS reports.report_product_quality;
DROP TABLE IF EXISTS reports.report_sales_by_supplier;
DROP TABLE IF EXISTS reports.report_sales_by_store;
DROP TABLE IF EXISTS reports.report_sales_by_time;
DROP TABLE IF EXISTS reports.report_sales_by_customer;
DROP TABLE IF EXISTS reports.report_sales_by_product;

CREATE TABLE reports.report_sales_by_product AS
WITH base AS (
    SELECT
        product_key,
        product_name,
        product_category,
        product_brand,
        count(*)::bigint AS sales_count,
        sum(sale_quantity)::bigint AS total_units_sold,
        sum(sale_total_price)::numeric(18, 2) AS source_revenue,
        sum(calculated_total_amount)::numeric(18, 2) AS calculated_revenue,
        avg(calculated_total_amount / sale_quantity)::numeric(18, 2) AS avg_unit_price,
        min(product_rating)::numeric(3, 1) AS product_rating,
        min(product_reviews)::integer AS product_reviews
    FROM stage.v_sales_typed
    GROUP BY product_key, product_name, product_category, product_brand
),
ranked AS (
    SELECT
        base.*,
        sum(source_revenue) OVER (PARTITION BY product_category)::numeric(18, 2) AS category_source_revenue,
        row_number() OVER (ORDER BY total_units_sold DESC, product_name, product_key)::bigint AS product_units_rank
    FROM base
)
SELECT
    row_number() OVER (ORDER BY product_name, product_key)::bigint AS report_row_id,
    product_key,
    product_name,
    product_category,
    product_brand,
    sales_count,
    total_units_sold,
    source_revenue,
    calculated_revenue,
    (source_revenue - calculated_revenue)::numeric(18, 2) AS revenue_delta,
    avg_unit_price,
    product_rating,
    product_reviews,
    category_source_revenue,
    product_units_rank,
    product_units_rank <= 10 AS is_top_10_by_units
FROM ranked;

CREATE TABLE reports.report_sales_by_customer AS
WITH base AS (
    SELECT
        customer_key,
        customer_email,
        concat(customer_first_name, ' ', customer_last_name) AS customer_name,
        customer_country,
        count(*)::bigint AS sales_count,
        sum(sale_quantity)::bigint AS total_units_bought,
        sum(sale_total_price)::numeric(18, 2) AS source_revenue,
        sum(calculated_total_amount)::numeric(18, 2) AS calculated_revenue,
        avg(sale_total_price)::numeric(18, 2) AS avg_check
    FROM stage.v_sales_typed
    GROUP BY customer_key, customer_email, customer_first_name, customer_last_name, customer_country
),
ranked AS (
    SELECT
        base.*,
        row_number() OVER (ORDER BY source_revenue DESC, customer_email, customer_key)::bigint AS customer_revenue_rank,
        count(*) OVER (PARTITION BY customer_country)::bigint AS country_customer_count
    FROM base
)
SELECT
    row_number() OVER (ORDER BY customer_email, customer_key)::bigint AS report_row_id,
    customer_key,
    customer_email,
    customer_name,
    customer_country,
    sales_count,
    total_units_bought,
    source_revenue,
    calculated_revenue,
    avg_check,
    customer_revenue_rank,
    customer_revenue_rank <= 10 AS is_top_10_by_revenue,
    country_customer_count
FROM ranked;

CREATE TABLE reports.report_sales_by_time AS
WITH base AS (
    SELECT
        extract(year FROM sale_date)::integer AS sales_year,
        extract(month FROM sale_date)::integer AS sales_month,
        date_trunc('month', sale_date)::date AS period_start,
        count(*)::bigint AS sales_count,
        sum(sale_quantity)::bigint AS total_units_sold,
        sum(sale_total_price)::numeric(18, 2) AS source_revenue,
        sum(calculated_total_amount)::numeric(18, 2) AS calculated_revenue,
        avg(sale_total_price)::numeric(18, 2) AS avg_order_amount
    FROM stage.v_sales_typed
    GROUP BY extract(year FROM sale_date), extract(month FROM sale_date), date_trunc('month', sale_date)
),
ranked AS (
    SELECT
        base.*,
        lag(source_revenue, 1, 0::numeric) OVER (ORDER BY period_start)::numeric(18, 2) AS prev_month_source_revenue
    FROM base
)
SELECT
    row_number() OVER (ORDER BY period_start)::bigint AS report_row_id,
    sales_year,
    sales_month,
    period_start,
    sales_count,
    total_units_sold,
    source_revenue,
    calculated_revenue,
    avg_order_amount,
    prev_month_source_revenue,
    (source_revenue - prev_month_source_revenue)::numeric(18, 2) AS source_revenue_delta
FROM ranked;

CREATE TABLE reports.report_sales_by_store AS
WITH base AS (
    SELECT
        store_key,
        store_name,
        store_city,
        store_country,
        count(*)::bigint AS sales_count,
        sum(sale_quantity)::bigint AS total_units_sold,
        sum(sale_total_price)::numeric(18, 2) AS source_revenue,
        sum(calculated_total_amount)::numeric(18, 2) AS calculated_revenue,
        avg(sale_total_price)::numeric(18, 2) AS avg_check
    FROM stage.v_sales_typed
    GROUP BY store_key, store_name, store_city, store_country
),
ranked AS (
    SELECT
        base.*,
        row_number() OVER (ORDER BY source_revenue DESC, store_name, store_key)::bigint AS store_revenue_rank,
        sum(source_revenue) OVER (PARTITION BY store_city)::numeric(18, 2) AS city_source_revenue,
        sum(source_revenue) OVER (PARTITION BY store_country)::numeric(18, 2) AS country_source_revenue
    FROM base
)
SELECT
    row_number() OVER (ORDER BY store_name, store_key)::bigint AS report_row_id,
    store_key,
    store_name,
    store_city,
    store_country,
    sales_count,
    total_units_sold,
    source_revenue,
    calculated_revenue,
    avg_check,
    store_revenue_rank,
    store_revenue_rank <= 5 AS is_top_5_by_revenue,
    city_source_revenue,
    country_source_revenue
FROM ranked;

CREATE TABLE reports.report_sales_by_supplier AS
WITH base AS (
    SELECT
        supplier_key,
        supplier_name,
        supplier_city,
        supplier_country,
        count(*)::bigint AS sales_count,
        sum(sale_quantity)::bigint AS total_units_sold,
        sum(sale_total_price)::numeric(18, 2) AS source_revenue,
        sum(calculated_total_amount)::numeric(18, 2) AS calculated_revenue,
        avg(calculated_total_amount / sale_quantity)::numeric(18, 2) AS avg_product_unit_price
    FROM stage.v_sales_typed
    GROUP BY supplier_key, supplier_name, supplier_city, supplier_country
),
ranked AS (
    SELECT
        base.*,
        row_number() OVER (ORDER BY source_revenue DESC, supplier_name, supplier_key)::bigint AS supplier_revenue_rank,
        sum(source_revenue) OVER (PARTITION BY supplier_country)::numeric(18, 2) AS country_source_revenue
    FROM base
)
SELECT
    row_number() OVER (ORDER BY supplier_name, supplier_key)::bigint AS report_row_id,
    supplier_key,
    supplier_name,
    supplier_city,
    supplier_country,
    sales_count,
    total_units_sold,
    source_revenue,
    calculated_revenue,
    avg_product_unit_price,
    supplier_revenue_rank,
    supplier_revenue_rank <= 5 AS is_top_5_by_revenue,
    country_source_revenue
FROM ranked;

CREATE TABLE reports.report_product_quality AS
WITH base AS (
    SELECT
        product_key,
        product_name,
        product_category,
        min(product_rating)::numeric(3, 1) AS product_rating,
        min(product_reviews)::integer AS product_reviews,
        sum(sale_quantity)::bigint AS total_units_sold,
        sum(sale_total_price)::numeric(18, 2) AS source_revenue
    FROM stage.v_sales_typed
    GROUP BY product_key, product_name, product_category
),
ranked AS (
    SELECT
        base.*,
        corr(product_rating::double precision, total_units_sold::double precision) OVER () AS rating_sales_correlation,
        row_number() OVER (ORDER BY product_rating DESC, product_reviews DESC, product_name, product_key)::bigint AS best_rating_rank,
        row_number() OVER (ORDER BY product_rating ASC, product_reviews DESC, product_name, product_key)::bigint AS worst_rating_rank,
        row_number() OVER (ORDER BY product_reviews DESC, product_rating DESC, product_name, product_key)::bigint AS reviews_rank
    FROM base
)
SELECT
    row_number() OVER (ORDER BY product_name, product_key)::bigint AS report_row_id,
    product_key,
    product_name,
    product_category,
    product_rating,
    product_reviews,
    total_units_sold,
    source_revenue,
    rating_sales_correlation,
    best_rating_rank,
    worst_rating_rank,
    reviews_rank,
    reviews_rank <= 10 AS is_top_10_by_reviews,
    best_rating_rank <= 10 AS is_top_10_by_rating,
    worst_rating_rank <= 10 AS is_bottom_10_by_rating
FROM ranked;

ANALYZE stage.sales_raw;
ANALYZE reports.report_sales_by_product;
ANALYZE reports.report_sales_by_customer;
ANALYZE reports.report_sales_by_time;
ANALYZE reports.report_sales_by_store;
ANALYZE reports.report_sales_by_supplier;
ANALYZE reports.report_product_quality;
