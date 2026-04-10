-- =====================================
-- SMART ETL ENGINE - SQL QUERIES
-- Table name: data
-- =====================================

-- 1️⃣ View full dataset
SELECT * FROM data;


-- 2️⃣ Count total rows
SELECT COUNT(*) AS total_rows FROM data;


-- 3️⃣ Check null values (basic check)
SELECT *
FROM data
WHERE price IS NULL OR quantity IS NULL;


-- 4️⃣ Total revenue (if column exists)
SELECT SUM(total_revenue) AS total_revenue
FROM data;


-- 5️⃣ Revenue by category
SELECT category,
       SUM(total_revenue) AS category_revenue
FROM data
GROUP BY category
ORDER BY category_revenue DESC;


-- 6️⃣ Top selling products (by quantity)
SELECT product,
       SUM(quantity) AS total_quantity
FROM data
GROUP BY product
ORDER BY total_quantity DESC
LIMIT 10;


-- 7️⃣ Average price per category
SELECT category,
       AVG(price) AS avg_price
FROM data
GROUP BY category;


-- 8️⃣ Highest revenue product
SELECT product,
       SUM(total_revenue) AS revenue
FROM data
GROUP BY product
ORDER BY revenue DESC
LIMIT 1;


-- 9️⃣ Category-wise quantity sold
SELECT category,
       SUM(quantity) AS total_quantity
FROM data
GROUP BY category;


-- 🔟 Data quality check (duplicate rows count idea)
SELECT product, category, price, quantity, COUNT(*) AS freq
FROM data
GROUP BY product, category, price, quantity
HAVING COUNT(*) > 1;