-- Based, but not equal to tpc-ds q6
-- List top 10 states in descending order with at least 10 customers who during
-- a given month bought products with the price tag at least 20% higher than the
-- average price of products in the same category.

-- Resources
-- helper table: items with 20% higher then avg prices of product from same category
DROP TABLE bigbench_tmp_table1;
CREATE COLUMN TABLE bigbench_tmp_table1 AS
-- "price tag at least 20% higher than the average price of products in the same category."
(SELECT k.i_item_sk
FROM import_cp_item k,
(
  SELECT
    AVG(j.i_current_price) * 1.2 AS avg_price
  FROM import_cp_item j
  GROUP BY j.i_category
) avgCategoryPrice
WHERE k.i_current_price > avgCategoryPrice.avg_price)
;

-- the real query part
SELECT
  ca_state,
  COUNT(*) AS cnt
FROM
  import_cp_customer_address a,
  import_customer c,
  import_cp_store_sales s,
  bigbench_tmp_table1 highPriceItems
WHERE a.ca_address_sk = c.c_current_addr_sk
AND c.c_customer_sk = s.ss_customer_sk
AND ca_state IS NOT NULL
AND highPriceItems.i_item_sk = ss_item_sk
AND s.ss_sold_date_sk IN
( --during a given month
  SELECT d_date_sk
  FROM import_date_dim
  WHERE d_year = 2004
  AND d_moy = 7
)
GROUP BY ca_state
HAVING COUNT(*) >= 10 --at least 10 customers
ORDER BY cnt DESC, ca_state
LIMIT 10
;

--cleanup
DROP TABLE bigbench_tmp_table1;