-- =============================================================================
-- sample_data/seed.sql
-- Realistic e-commerce database with intentional data quality issues baked in.
-- The AI Data Quality Agent will find and report all of these.
--
-- Issues planted:
--   1. orders.amount has outliers (some orders $0, one order $999999)
--   2. customers.email has ~8% null rate (above typical threshold)
--   3. products.stock_count has negative values (impossible)
--   4. order_items.quantity = 0 on some rows (invalid)
--   5. customers.signup_date has future dates (data entry errors)
--   6. orders.status has inconsistent casing (COMPLETED vs completed)
-- =============================================================================

-- Customers table
CREATE TABLE IF NOT EXISTS customers (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    email       VARCHAR(150),          -- nullable: ~8% null rate
    country     VARCHAR(50),
    signup_date DATE,
    plan        VARCHAR(20) DEFAULT 'free'
);

-- Products table
CREATE TABLE IF NOT EXISTS products (
    id           SERIAL PRIMARY KEY,
    name         VARCHAR(200) NOT NULL,
    category     VARCHAR(50),
    price        NUMERIC(10,2),
    stock_count  INTEGER,              -- has negative values (data quality issue)
    created_at   TIMESTAMP DEFAULT NOW()
);

-- Orders table
CREATE TABLE IF NOT EXISTS orders (
    id          SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(id),
    amount      NUMERIC(12,2),         -- has outliers: $0 and $999999
    status      VARCHAR(30),           -- inconsistent casing issue
    created_at  TIMESTAMP DEFAULT NOW()
);

-- Order items table
CREATE TABLE IF NOT EXISTS order_items (
    id         SERIAL PRIMARY KEY,
    order_id   INTEGER REFERENCES orders(id),
    product_id INTEGER REFERENCES products(id),
    quantity   INTEGER,               -- some zero quantities
    unit_price NUMERIC(10,2)
);

-- ── Seed Data ──────────────────────────────────────────────────────────────────

-- Customers (600 rows, ~8% null email)
INSERT INTO customers (name, email, country, signup_date, plan)
SELECT
    'Customer ' || i,
    CASE WHEN i % 13 = 0 THEN NULL ELSE 'customer' || i || '@email.com' END,
    (ARRAY['US','UK','CA','AU','DE','FR','IN','BR'])[1 + (i % 8)],
    CASE
        WHEN i % 50 = 0 THEN CURRENT_DATE + INTERVAL '30 days'  -- future dates (bug)
        ELSE CURRENT_DATE - (random() * 730)::int * INTERVAL '1 day'
    END,
    (ARRAY['free','pro','enterprise'])[1 + (i % 3)]
FROM generate_series(1, 600) AS i;

-- Products (80 rows, some with negative stock)
INSERT INTO products (name, category, price, stock_count)
SELECT
    'Product ' || i,
    (ARRAY['Electronics','Clothing','Books','Home','Sports','Beauty'])[1 + (i % 6)],
    ROUND((random() * 500 + 5)::numeric, 2),
    CASE
        WHEN i % 15 = 0 THEN -1 * (i % 20 + 1)  -- negative stock (impossible)
        ELSE (random() * 1000)::int
    END
FROM generate_series(1, 80) AS i;

-- Orders (2000 rows, with outliers in amount + status casing issues)
INSERT INTO orders (customer_id, amount, status, created_at)
SELECT
    1 + (random() * 599)::int,
    CASE
        WHEN i = 42   THEN 999999.99   -- extreme outlier
        WHEN i % 30 = 0 THEN 0.00     -- zero orders
        ELSE ROUND((random() * 800 + 20)::numeric, 2)
    END,
    CASE (i % 5)
        WHEN 0 THEN 'completed'        -- lowercase
        WHEN 1 THEN 'COMPLETED'        -- UPPERCASE (inconsistency)
        WHEN 2 THEN 'pending'
        WHEN 3 THEN 'Cancelled'        -- Title case (3rd variant)
        ELSE        'shipped'
    END,
    NOW() - (random() * 365)::int * INTERVAL '1 day'
FROM generate_series(1, 2000) AS i;

-- Order items (5000 rows, some zero quantities)
INSERT INTO order_items (order_id, product_id, quantity, unit_price)
SELECT
    1 + (random() * 1999)::int,
    1 + (random() * 79)::int,
    CASE WHEN i % 40 = 0 THEN 0 ELSE 1 + (random() * 9)::int END,
    ROUND((random() * 300 + 5)::numeric, 2)
FROM generate_series(1, 5000) AS i;

-- ── Summary view for easy inspection ────────────────────────────────────────
CREATE VIEW data_quality_summary AS
SELECT
    'customers'   AS table_name, COUNT(*) AS row_count,
    SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) AS null_emails,
    NULL::bigint AS negative_stock, NULL::bigint AS zero_quantity
FROM customers
UNION ALL
SELECT 'products', COUNT(*), NULL,
    SUM(CASE WHEN stock_count < 0 THEN 1 ELSE 0 END), NULL
FROM products
UNION ALL
SELECT 'order_items', COUNT(*), NULL, NULL,
    SUM(CASE WHEN quantity = 0 THEN 1 ELSE 0 END)
FROM order_items;
