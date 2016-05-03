-- based on tpc-ds q29
-- Get all items that were sold in stores in a given month
-- and year and which were returned in the next 6 months and re-purchased by
-- the returning customer afterwards through the web sales channel in the following
-- three years. For those items, compute the total quantity sold through the
-- store, the quantity returned and the quantity purchased through the web. Group
-- this information by item and store.

-- Resources
-- the real query part
SELECT
  i_item_id,
  i_item_desc,
  s_store_id,
  s_store_name,
  SUM(ss_quantity) AS store_sales_quantity,
  SUM(sr_return_quantity) AS store_returns_quantity,
  SUM(ws_quantity) AS web_sales_quantity
FROM
  import_cp_store_sales,
  import_cp_store_returns,
  import_cp_web_sales,
  import_date_dim d1,
  import_date_dim d2,
  import_date_dim d3,
  import_cp_store,
  import_cp_item
WHERE d1.d_year         = 2003 --sold in stores in a given month and year
AND d1.d_moy            = 1
AND d1.d_date_sk        = ss_sold_date_sk
AND i_item_sk           = ss_item_sk
AND s_store_sk          = ss_store_sk
AND ss_customer_sk      = sr_customer_sk
AND ss_item_sk          = sr_item_sk
AND ss_ticket_number    = sr_ticket_number
AND sr_returned_date_sk = d2.d_date_sk
AND d2.d_moy BETWEEN 1 AND 1 + 6 --which were returned in the next six months 
AND d2.d_year           = 2003
AND sr_customer_sk      = ws_bill_customer_sk --re-purchased by the returning customer afterwards through the web sales channel
AND sr_item_sk          = ws_item_sk
AND ws_sold_date_sk     = d3.d_date_sk
AND d3.d_year BETWEEN 2003 AND 2003 + 2 -- in the following three years (re-purchased by the returning customer afterwards through the web sales channel)
GROUP BY
  i_item_id,
  i_item_desc,
  s_store_id,
  s_store_name
ORDER BY
  i_item_id,
  i_item_desc,
  s_store_id,
  s_store_name
LIMIT 100;