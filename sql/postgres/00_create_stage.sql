CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS reports;

CREATE OR REPLACE FUNCTION stage.parse_source_date(value text)
RETURNS date
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE
        WHEN NULLIF(value, '') IS NULL THEN NULL
        WHEN value ~ '^[0-9]{12,}$' THEN to_timestamp(value::numeric / 1000)::date
        ELSE to_date(value, 'MM/DD/YYYY')
    END
$$;

DROP TABLE IF EXISTS stage.sales_raw CASCADE;

CREATE TABLE stage.sales_raw (
    raw_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_file text NOT NULL,
    id text,
    customer_first_name text,
    customer_last_name text,
    customer_age text,
    customer_email text,
    customer_country text,
    customer_postal_code text,
    customer_pet_type text,
    customer_pet_name text,
    customer_pet_breed text,
    seller_first_name text,
    seller_last_name text,
    seller_email text,
    seller_country text,
    seller_postal_code text,
    product_name text,
    product_category text,
    product_price text,
    product_quantity text,
    sale_date text,
    sale_customer_id text,
    sale_seller_id text,
    sale_product_id text,
    sale_quantity text,
    sale_total_price text,
    store_name text,
    store_location text,
    store_city text,
    store_state text,
    store_country text,
    store_phone text,
    store_email text,
    pet_category text,
    product_weight text,
    product_color text,
    product_size text,
    product_brand text,
    product_material text,
    product_description text,
    product_rating text,
    product_reviews text,
    product_release_date text,
    product_expiry_date text,
    supplier_name text,
    supplier_contact text,
    supplier_email text,
    supplier_phone text,
    supplier_address text,
    supplier_city text,
    supplier_country text,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_sales_raw_source_row UNIQUE (source_file, id)
);

CREATE OR REPLACE VIEW stage.v_sales_typed AS
SELECT
    raw_id,
    source_file,
    md5(concat_ws('|', source_file, id)) AS sale_event_key,
    NULLIF(id, '')::integer AS source_id,
    md5(lower(NULLIF(customer_email, ''))) AS customer_key,
    NULLIF(customer_first_name, '') AS customer_first_name,
    NULLIF(customer_last_name, '') AS customer_last_name,
    NULLIF(customer_age, '')::integer AS customer_age,
    lower(NULLIF(customer_email, '')) AS customer_email,
    NULLIF(customer_country, '') AS customer_country,
    NULLIF(customer_postal_code, '') AS customer_postal_code,
    md5(concat_ws('|', NULLIF(customer_pet_name, ''), lower(NULLIF(customer_pet_type, '')), NULLIF(customer_pet_breed, ''), NULLIF(pet_category, ''))) AS pet_key,
    lower(NULLIF(customer_pet_type, '')) AS customer_pet_type,
    NULLIF(customer_pet_name, '') AS customer_pet_name,
    NULLIF(customer_pet_breed, '') AS customer_pet_breed,
    NULLIF(pet_category, '') AS pet_category,
    md5(lower(NULLIF(seller_email, ''))) AS seller_key,
    NULLIF(seller_first_name, '') AS seller_first_name,
    NULLIF(seller_last_name, '') AS seller_last_name,
    lower(NULLIF(seller_email, '')) AS seller_email,
    NULLIF(seller_country, '') AS seller_country,
    NULLIF(seller_postal_code, '') AS seller_postal_code,
    md5(concat_ws('|', NULLIF(product_name, ''), NULLIF(product_category, ''), NULLIF(product_brand, ''), NULLIF(product_material, ''), NULLIF(product_color, ''), NULLIF(product_size, ''), NULLIF(product_weight, ''), NULLIF(product_description, ''))) AS product_key,
    NULLIF(product_name, '') AS product_name,
    NULLIF(product_category, '') AS product_category,
    NULLIF(product_price, '')::numeric(14, 2) AS product_price,
    NULLIF(product_quantity, '')::integer AS product_quantity,
    NULLIF(product_weight, '')::numeric(12, 2) AS product_weight,
    NULLIF(product_color, '') AS product_color,
    NULLIF(product_size, '') AS product_size,
    NULLIF(product_brand, '') AS product_brand,
    NULLIF(product_material, '') AS product_material,
    NULLIF(product_description, '') AS product_description,
    NULLIF(product_rating, '')::numeric(3, 1) AS product_rating,
    NULLIF(product_reviews, '')::integer AS product_reviews,
    stage.parse_source_date(product_release_date) AS product_release_date,
    stage.parse_source_date(product_expiry_date) AS product_expiry_date,
    stage.parse_source_date(sale_date) AS sale_date,
    NULLIF(sale_customer_id, '')::integer AS sale_customer_id,
    NULLIF(sale_seller_id, '')::integer AS sale_seller_id,
    NULLIF(sale_product_id, '')::integer AS sale_product_id,
    NULLIF(sale_quantity, '')::integer AS sale_quantity,
    NULLIF(sale_total_price, '')::numeric(14, 2) AS sale_total_price,
    round(NULLIF(product_price, '')::numeric(14, 2) * NULLIF(sale_quantity, '')::numeric(14, 2), 2) AS calculated_total_amount,
    abs(NULLIF(sale_total_price, '')::numeric(14, 2) - NULLIF(product_price, '')::numeric(14, 2) * NULLIF(sale_quantity, '')::numeric(14, 2)) <= 0.01 AS is_total_consistent,
    md5(lower(NULLIF(store_email, ''))) AS store_key,
    NULLIF(store_name, '') AS store_name,
    NULLIF(store_location, '') AS store_location,
    NULLIF(store_city, '') AS store_city,
    NULLIF(store_state, '') AS store_state,
    NULLIF(store_country, '') AS store_country,
    NULLIF(store_phone, '') AS store_phone,
    lower(NULLIF(store_email, '')) AS store_email,
    md5(lower(NULLIF(supplier_email, ''))) AS supplier_key,
    NULLIF(supplier_name, '') AS supplier_name,
    NULLIF(supplier_contact, '') AS supplier_contact,
    lower(NULLIF(supplier_email, '')) AS supplier_email,
    NULLIF(supplier_phone, '') AS supplier_phone,
    NULLIF(supplier_address, '') AS supplier_address,
    NULLIF(supplier_city, '') AS supplier_city,
    NULLIF(supplier_country, '') AS supplier_country
FROM stage.sales_raw;

CREATE INDEX ix_sales_raw_source_file ON stage.sales_raw (source_file);
CREATE INDEX ix_sales_raw_product ON stage.sales_raw (product_name, product_category);
