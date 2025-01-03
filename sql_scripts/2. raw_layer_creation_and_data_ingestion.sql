------
-- Create database and schemas
------

USE DATABASE PRD_ECOMMERCE_DB;

-- 1. RAW.REGIONS
CREATE OR REPLACE TABLE PRD_ECOMMERCE_DB.RAW.REGIONS (
    region_id INTEGER,
    region_name VARCHAR(50),
    region_code VARCHAR(10),
    country_code VARCHAR(2),
    is_active BOOLEAN,
    valid_from TIMESTAMP_NTZ,
    valid_to TIMESTAMP_NTZ
);

-- 2. RAW.CUSTOMERS
CREATE OR REPLACE TABLE PRD_ECOMMERCE_DB.RAW.CUSTOMERS (
    customer_id VARCHAR(16),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(20),
    date_of_birth DATE,
    region_id INTEGER,
    address_line1 VARCHAR(100),
    address_line2 VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(2),
    postal_code VARCHAR(10),
    country VARCHAR(2),
    customer_segment VARCHAR(20),
    is_active BOOLEAN,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    last_login_at TIMESTAMP_NTZ
);

-- 3. RAW.PRODUCT_CATEGORIES
CREATE OR REPLACE TABLE PRD_ECOMMERCE_DB.RAW.PRODUCT_CATEGORIES (
    category_id INTEGER,
    parent_category_id INTEGER,
    category_name VARCHAR(50),
    category_code VARCHAR(20),
    category_description TEXT,
    is_active BOOLEAN,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ
);

-- 4. RAW.PRODUCTS
CREATE OR REPLACE TABLE PRD_ECOMMERCE_DB.RAW.PRODUCTS (
    product_id VARCHAR(16),
    product_name VARCHAR(100),
    product_code VARCHAR(50),
    category_id INTEGER,
    description TEXT,
    base_price_cents INTEGER,
    discount_price_cents INTEGER,
    brand VARCHAR(50),
    weight_grams FLOAT,
    is_digital BOOLEAN,
    stock_quantity INTEGER,
    reorder_point INTEGER,
    is_active BOOLEAN,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ
);

-- 5. RAW.ORDERS
CREATE OR REPLACE TABLE PRD_ECOMMERCE_DB.RAW.ORDERS (
    order_id VARCHAR(16),
    customer_id VARCHAR(16),
    order_date TIMESTAMP_NTZ,
    order_status VARCHAR(20),
    payment_status VARCHAR(20),
    payment_method VARCHAR(20),
    shipping_method VARCHAR(20),
    currency_code VARCHAR(3),
    subtotal_cents INTEGER,
    shipping_amount_cents INTEGER,
    tax_amount_cents INTEGER,
    discount_amount_cents INTEGER,
    total_amount_cents INTEGER,
    estimated_delivery_date DATE,
    actual_delivery_date DATE,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ
);

-- 6. RAW.ORDER_ITEMS
CREATE OR REPLACE TABLE PRD_ECOMMERCE_DB.RAW.ORDER_ITEMS (
    order_item_id VARCHAR(16),
    order_id VARCHAR(16),
    product_id VARCHAR(16),
    quantity INTEGER,
    unit_price_cents INTEGER,
    discount_cents INTEGER,
    total_price_cents INTEGER,
    is_gift BOOLEAN,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ
);

-- 7. RAW.PROMOTIONS
CREATE OR REPLACE TABLE PRD_ECOMMERCE_DB.RAW.PROMOTIONS (
    promotion_id VARCHAR(16),
    promotion_code VARCHAR(20),
    promotion_name VARCHAR(100),
    description TEXT,
    discount_type VARCHAR(20),
    discount_value FLOAT,
    minimum_order_cents INTEGER,
    start_date TIMESTAMP_NTZ,
    end_date TIMESTAMP_NTZ,
    is_active BOOLEAN,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ
);

-- 8. RAW.ORDER_PROMOTIONS
CREATE OR REPLACE TABLE PRD_ECOMMERCE_DB.RAW.ORDER_PROMOTIONS (
    order_id VARCHAR(16),
    promotion_id VARCHAR(16),
    discount_amount_cents INTEGER,
    created_at TIMESTAMP_NTZ
);


------
-- Now let's generate sample data for each table:
------

-- Insert sample data into REGIONS
INSERT INTO PRD_ECOMMERCE_DB.RAW.REGIONS 
VALUES
    (1, 'North America', 'NA', 'US', TRUE, '2023-01-01', '9999-12-31'),
    (2, 'Europe', 'EU', 'GB', TRUE, '2023-01-01', '9999-12-31'),
    (3, 'Asia Pacific', 'APAC', 'SG', TRUE, '2023-01-01', '9999-12-31'),
    (4, 'Latin America', 'LATAM', 'BR', TRUE, '2023-01-01', '9999-12-31'),
    (5, 'Middle East', 'ME', 'AE', TRUE, '2023-01-01', '9999-12-31');

-- Insert sample data into PRODUCT_CATEGORIES
INSERT INTO PRD_ECOMMERCE_DB.RAW.PRODUCT_CATEGORIES
VALUES
    (1, NULL, 'Electronics', 'ELECT', 'Electronic devices and accessories', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    (2, 1, 'Smartphones', 'PHONE', 'Mobile phones and accessories', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    (3, 1, 'Laptops', 'LAPTOP', 'Laptops and accessories', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    (4, NULL, 'Fashion', 'FASH', 'Clothing and accessories', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    (5, 4, 'Men''s Wear', 'MENW', 'Men''s clothing', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- Insert sample data into PRODUCTS
INSERT INTO PRD_ECOMMERCE_DB.RAW.PRODUCTS 
SELECT
    'PROD' || LPAD(SEQ4(), 8, '0'),
    ARRAY_CONSTRUCT(
        'iPhone 13 Pro', 'MacBook Air', 'Samsung Galaxy S21', 'Dell XPS 13',
        'Leather Wallet', 'Wireless Earbuds', 'Smart Watch', 'Gaming Mouse',
        'Mechanical Keyboard', 'USB-C Hub'
    )[UNIFORM(1, 10, RANDOM())],
    'SKU-' || LPAD(SEQ4(), 6, '0'),
    UNIFORM(1, 5, RANDOM()),
    'High-quality product with premium features',
    UNIFORM(29900, 199900, RANDOM()),
    NULL,
    ARRAY_CONSTRUCT('Apple', 'Samsung', 'Dell', 'Sony', 'LG')[UNIFORM(1, 5, RANDOM())],
    UNIFORM(100, 2000, RANDOM()),
    FALSE,
    UNIFORM(10, 100, RANDOM()),
    UNIFORM(5, 20, RANDOM()),
    TRUE,
    DATEADD(day, -UNIFORM(1, 365, RANDOM()), CURRENT_TIMESTAMP()),
    CURRENT_TIMESTAMP()
FROM TABLE(GENERATOR(ROWCOUNT => 50));

-- Insert sample data into CUSTOMERS
INSERT INTO PRD_ECOMMERCE_DB.RAW.CUSTOMERS 
WITH names AS (
    SELECT
        ARRAY_CONSTRUCT('John', 'Jane', 'Michael', 'Sarah', 'David')[UNIFORM(1, 5, RANDOM())] as first_name,
        ARRAY_CONSTRUCT('Smith', 'Johnson', 'Williams', 'Brown', 'Jones')[UNIFORM(1, 5, RANDOM())] as last_name
    FROM TABLE(GENERATOR(ROWCOUNT => 50))
)
SELECT
    'CUST' || LPAD(SEQ4(), 8, '0') as customer_id,
    names.first_name,
    names.last_name,
    LOWER(names.first_name || '.' || names.last_name || '@email.com') as email,
    '555-' || LPAD(UNIFORM(100, 999, RANDOM()), 3, '0') || '-' || LPAD(UNIFORM(1000, 9999, RANDOM()), 4, '0') as phone,
    DATEADD(year, -UNIFORM(20, 60, RANDOM()), CURRENT_DATE()) as date_of_birth,
    UNIFORM(1, 5, RANDOM()) as region_id,
    ARRAY_CONSTRUCT('123 Main St', '456 Oak Ave', '789 Pine Rd')[UNIFORM(1, 3, RANDOM())] as address_line1,
    NULL as address_line2,
    ARRAY_CONSTRUCT('New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix')[UNIFORM(1, 5, RANDOM())] as city,
    ARRAY_CONSTRUCT('NY', 'CA', 'IL', 'TX', 'AZ')[UNIFORM(1, 5, RANDOM())] as state,
    LPAD(UNIFORM(10000, 99999, RANDOM()), 5, '0') as postal_code,
    'US' as country,
    ARRAY_CONSTRUCT('REGULAR', 'PREMIUM', 'VIP')[UNIFORM(1, 3, RANDOM())] as customer_segment,
    TRUE as is_active,
    DATEADD(day, -UNIFORM(1, 1000, RANDOM()), CURRENT_TIMESTAMP()) as created_at,
    CURRENT_TIMESTAMP() as updated_at,
    DATEADD(day, -UNIFORM(1, 30, RANDOM()), CURRENT_TIMESTAMP()) as last_login_at
FROM names;

-- Insert sample data into ORDERS
INSERT INTO PRD_ECOMMERCE_DB.RAW.ORDERS 
WITH base_data AS (
    SELECT
        'ORD' || LPAD(SEQ4(), 8, '0') as order_id,
        (SELECT customer_id FROM PRD_ECOMMERCE_DB.RAW.CUSTOMERS ORDER BY RANDOM() LIMIT 1) as customer_id,
        DATEADD(day, -UNIFORM(1, 180, RANDOM()), CURRENT_TIMESTAMP()) as order_date,
        ARRAY_CONSTRUCT('COMPLETED', 'SHIPPED', 'PROCESSING', 'CANCELLED')[UNIFORM(1, 4, RANDOM())] as order_status,
        ARRAY_CONSTRUCT('PAID', 'PENDING', 'FAILED')[UNIFORM(1, 3, RANDOM())] as payment_status,
        ARRAY_CONSTRUCT('CREDIT_CARD', 'PAYPAL', 'BANK_TRANSFER')[UNIFORM(1, 3, RANDOM())] as payment_method,
        ARRAY_CONSTRUCT('STANDARD', 'EXPRESS', 'NEXT_DAY')[UNIFORM(1, 3, RANDOM())] as shipping_method,
        'USD' as currency_code,
        UNIFORM(1000, 100000, RANDOM()) as subtotal_cents,
        UNIFORM(500, 2000, RANDOM()) as shipping_amount_cents,
        UNIFORM(100, 5000, RANDOM()) as tax_amount_cents,
        0 as discount_amount_cents,
        0 as total_amount_cents
    FROM TABLE(GENERATOR(ROWCOUNT => 50))
)
SELECT
    order_id,
    customer_id,
    order_date,
    order_status,
    payment_status,
    payment_method,
    shipping_method,
    currency_code,
    subtotal_cents,
    shipping_amount_cents,
    tax_amount_cents,
    discount_amount_cents,
    total_amount_cents,
    DATEADD(day, UNIFORM(3, 7, RANDOM()), order_date) as estimated_delivery_date,
    NULL as actual_delivery_date,
    order_date as created_at,
    CURRENT_TIMESTAMP() as updated_at
FROM base_data;

-- Insert sample data into ORDER_ITEMS 
INSERT INTO PRD_ECOMMERCE_DB.RAW.ORDER_ITEMS 
WITH base_items AS (
    SELECT
        'ITEM' || LPAD(SEQ4(), 8, '0') as order_item_id,
        (SELECT order_id FROM PRD_ECOMMERCE_DB.RAW.ORDERS ORDER BY RANDOM() LIMIT 1) as order_id,
        (SELECT product_id FROM PRD_ECOMMERCE_DB.RAW.PRODUCTS ORDER BY RANDOM() LIMIT 1) as product_id,
        UNIFORM(1, 5, RANDOM()) as quantity
    FROM TABLE(GENERATOR(ROWCOUNT => 100))
),
items_with_prices AS (
    SELECT 
        bi.*,
        p.base_price_cents as unit_price_cents
    FROM base_items bi
    JOIN PRD_ECOMMERCE_DB.RAW.PRODUCTS p ON p.product_id = bi.product_id
)
SELECT
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price_cents,
    0 as discount_cents,
    quantity * unit_price_cents as total_price_cents,
    RANDOM() < 0.2 as is_gift,  -- 20% chance of being a gift
    CURRENT_TIMESTAMP() as created_at,
    CURRENT_TIMESTAMP() as updated_at
FROM items_with_prices;

-- Update order totals
UPDATE PRD_ECOMMERCE_DB.RAW.ORDERS o
SET 
    discount_amount_cents = COALESCE((
        SELECT SUM(discount_amount_cents)
        FROM PRD_ECOMMERCE_DB.RAW.ORDER_PROMOTIONS op
        WHERE op.order_id = o.order_id
    ), 0),
    total_amount_cents = subtotal_cents + shipping_amount_cents + tax_amount_cents - discount_amount_cents;

-- Insert sample PROMOTIONS
INSERT INTO PRD_ECOMMERCE_DB.RAW.PROMOTIONS
VALUES
    ('PROMO001', 'SUMMER2023', 'Summer Sale', '20% off on all electronics', 'PERCENTAGE', 20, 5000, '2023-06-01', '2023-08-31', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('PROMO002', 'NEWUSER', 'New User Discount', '$10 off on first purchase', 'FIXED', 10, 2000, '2023-01-01', '2023-12-31', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('PROMO003', 'HOLIDAY', 'Holiday Special', '15% off on orders above $100', 'PERCENTAGE', 15, 10000, '2023-12-01', '2023-12-25', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- Insert sample ORDER_PROMOTIONS
INSERT INTO PRD_ECOMMERCE_DB.RAW.ORDER_PROMOTIONS
SELECT
    o.order_id,
    p.promotion_id,
    CASE 
        WHEN p.discount_type = 'PERCENTAGE' 
        THEN FLOOR(o.subtotal_cents * p.discount_value / 100)
        ELSE p.discount_value * 100
    END as discount_amount_cents,
    CURRENT_TIMESTAMP() as created_at
FROM PRD_ECOMMERCE_DB.RAW.ORDERS o
CROSS JOIN PRD_ECOMMERCE_DB.RAW.PROMOTIONS p
WHERE RANDOM() < 0.3  -- 30% chance of order having a promotion
AND o.subtotal_cents >= p.minimum_order_cents;





-- Check row counts
SELECT 
    'REGIONS' as table_name, COUNT(*) as row_count FROM PRD_ECOMMERCE_DB.RAW.REGIONS
UNION ALL
SELECT 'CUSTOMERS', COUNT(*) FROM PRD_ECOMMERCE_DB.RAW.CUSTOMERS
UNION ALL
SELECT 'PRODUCT_CATEGORIES', COUNT(*) FROM PRD_ECOMMERCE_DB.RAW.PRODUCT_CATEGORIES
UNION ALL
SELECT 'PRODUCTS', COUNT(*) FROM PRD_ECOMMERCE_DB.RAW.PRODUCTS
UNION ALL
SELECT 'ORDERS', COUNT(*) FROM PRD_ECOMMERCE_DB.RAW.ORDERS
UNION ALL
SELECT 'ORDER_ITEMS', COUNT(*) FROM PRD_ECOMMERCE_DB.RAW.ORDER_ITEMS
UNION ALL
SELECT 'PROMOTIONS', COUNT(*) FROM PRD_ECOMMERCE_DB.RAW.PROMOTIONS
UNION ALL
SELECT 'ORDER_PROMOTIONS', COUNT(*) FROM PRD_ECOMMERCE_DB.RAW.ORDER_PROMOTIONS;




-----
-- Run this to replicate schemas and data from PRD to DEV and STG environments
-----
-- CREATE OR REPLACE SCHEMA DEV_ECOMMERCE_DB.RAW CLONE PRD_ECOMMERCE_DB.RAW;
-- CREATE OR REPLACE SCHEMA STG_ECOMMERCE_DB.RAW CLONE PRD_ECOMMERCE_DB.RAW;

