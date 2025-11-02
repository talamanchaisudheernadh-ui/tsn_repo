-- Databricks Delta Live Tables SQL pipeline example
-- Creates sample data, cleans it, and aggregates results

-- Step 1: Create Raw Table
CREATE OR REFRESH LIVE TABLE raw_customers
COMMENT "Raw customer data"
AS SELECT * FROM VALUES
  (1, 'Alice', 'Bangalore', 'Active', '2025-11-01'),
  (2, 'Bob', 'Hyderabad', 'Inactive', '2025-10-25'),
  (3, 'Charlie', 'Chennai', 'Active', '2025-10-28'),
  (4, 'David', 'Pune', 'Active', '2025-11-02'),
  (5, 'Eva', 'Delhi', 'Inactive', '2025-10-30')
AS t(customer_id, customer_name, city, status, update_date);

-- Step 2: Cleaned / Filtered Data
CREATE OR REFRESH LIVE TABLE clean_customers
COMMENT "Cleaned and standardized active customers"
AS
SELECT
  customer_id,
  INITCAP(customer_name) AS customer_name,
  UPPER(city) AS city,
  status,
  TO_DATE(update_date) AS update_date
FROM LIVE.raw_customers
WHERE status = 'Active';

-- Step 3: Aggregation / Summary Table
CREATE OR REFRESH LIVE TABLE customer_summary
COMMENT "City-wise active customer count"
AS
SELECT
  city,
  COUNT(*) AS active_customer_count
FROM LIVE.clean_customers
GROUP BY city;
