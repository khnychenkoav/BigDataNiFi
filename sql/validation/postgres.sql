SELECT 'stage.sales_raw' AS object_name, count(*) AS rows_count FROM stage.sales_raw
UNION ALL SELECT 'reports.report_product_quality', count(*) FROM reports.report_product_quality
UNION ALL SELECT 'reports.report_sales_by_customer', count(*) FROM reports.report_sales_by_customer
UNION ALL SELECT 'reports.report_sales_by_product', count(*) FROM reports.report_sales_by_product
UNION ALL SELECT 'reports.report_sales_by_store', count(*) FROM reports.report_sales_by_store
UNION ALL SELECT 'reports.report_sales_by_supplier', count(*) FROM reports.report_sales_by_supplier
UNION ALL SELECT 'reports.report_sales_by_time', count(*) FROM reports.report_sales_by_time
ORDER BY object_name;

SELECT
    count(*) AS raw_rows,
    count(DISTINCT source_file) AS source_files,
    sum(is_total_consistent::integer) AS consistent_total_rows,
    count(*) - sum(is_total_consistent::integer) AS inconsistent_total_rows,
    min(sale_total_price) AS min_source_total,
    max(sale_total_price) AS max_source_total,
    min(calculated_total_amount) AS min_calculated_total,
    max(calculated_total_amount) AS max_calculated_total
FROM stage.v_sales_typed;

SELECT source_file, count(*) AS rows_count
FROM stage.sales_raw
GROUP BY source_file
ORDER BY source_file;

SELECT 'product_top_10' AS check_name, count(*) AS marked_rows
FROM reports.report_sales_by_product
WHERE is_top_10_by_units
UNION ALL SELECT 'customer_top_10', count(*)
FROM reports.report_sales_by_customer
WHERE is_top_10_by_revenue
UNION ALL SELECT 'store_top_5', count(*)
FROM reports.report_sales_by_store
WHERE is_top_5_by_revenue
UNION ALL SELECT 'supplier_top_5', count(*)
FROM reports.report_sales_by_supplier
WHERE is_top_5_by_revenue
UNION ALL SELECT 'quality_top_reviews', count(*)
FROM reports.report_product_quality
WHERE is_top_10_by_reviews
ORDER BY check_name;
