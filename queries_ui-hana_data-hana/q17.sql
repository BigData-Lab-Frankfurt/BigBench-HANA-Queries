-- based on tpc-ds q61
-- Find the ratio of items sold with and without promotions
-- in a given month and year. Only items in certain categories sold to customers
-- living in a specific time zone are considered.

-- Resources
-- the real query part
-- no need to cast promotions or total to double: SUM(COL) already returned a DOUBLE
SELECT promotions, total, promotions / total * 100
FROM (
  SELECT SUM(ss_ext_sales_price) promotions
  FROM import_cp_store_sales ss
  JOIN import_date_dim dd ON ss.ss_sold_date_sk = dd.d_date_sk
  JOIN import_cp_item i ON ss.ss_item_sk = i.i_item_sk
  JOIN import_cp_store s ON ss.ss_store_sk = s.s_store_sk
  JOIN import_cp_promotion p ON ss.ss_promo_sk = p.p_promo_sk
  JOIN import_customer c ON ss.ss_customer_sk = c.c_customer_sk
  JOIN import_cp_customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
  WHERE ca_gmt_offset = -5
  AND s_gmt_offset = -5
  AND i_category IN ('Books', 'Music')
  AND d_year = 2001
  AND d_moy = 12
  AND (p_channel_dmail = 'Y' OR p_channel_email = 'Y' OR p_channel_tv = 'Y')
) promotional_sales, (
  SELECT SUM(ss_ext_sales_price) total
  FROM import_cp_store_sales ss
  JOIN import_date_dim dd ON ss.ss_sold_date_sk = dd.d_date_sk
  JOIN import_cp_item i ON ss.ss_item_sk = i.i_item_sk 
  JOIN import_cp_store s ON ss.ss_store_sk = s.s_store_sk
  JOIN import_cp_promotion p ON ss.ss_promo_sk = p.p_promo_sk
  JOIN import_customer c ON ss.ss_customer_sk = c.c_customer_sk
  JOIN import_cp_customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
  WHERE ca_gmt_offset = -5
  AND s_gmt_offset = -5
  AND i_category IN ('Books', 'Music')
  AND d_year = 2001
  AND d_moy = 12
) all_sales
-- we don't need a 'ON' join condition. result is just two numbers.
ORDER BY promotions, total
LIMIT 100 -- kinda useless, result is one line with two numbers, but original tpc-ds query has it too.
;