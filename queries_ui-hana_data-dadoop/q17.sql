-- based on tpc-ds q61
-- Find the ratio of items sold with and without promotions
-- in a given month and year. Only items in certain categories sold to customers
-- living in a specific time zone are considered.

-- Resources
-- the real query part
-- no need to cast promotions or total to double: SUM(COL) already returned a DOUBLE
SELECT promotions, total, promotions / total * 100
FROM (
  SELECT SUM("ss"."ss_ext_sales_price") promotions
  FROM "SYSTEM"."SPARK_cp_store_sales" "ss"
  JOIN "SYSTEM"."SPARK_date_dim" "dd" ON "ss"."ss_sold_date_sk" = "dd"."d_date_sk"
  JOIN "SYSTEM"."SPARK_cp_item" "i" ON "ss"."ss_item_sk" = "i"."i_item_sk"
  JOIN "SYSTEM"."SPARK_cp_store" "s" ON "ss"."ss_store_sk" = "s"."s_store_sk"
  JOIN "SYSTEM"."SPARK_cp_promotion" "p" ON "ss"."ss_promo_sk" = "p"."p_promo_sk"
  JOIN "SYSTEM"."SPARK_customer" "c" ON "ss"."ss_customer_sk" = "c"."c_customer_sk"
  JOIN "SYSTEM"."SPARK_cp_customer_address" "ca" ON "c"."c_current_addr_sk" = "ca"."ca_address_sk"
  WHERE "ca"."ca_gmt_offset" = -5
  AND "s"."s_gmt_offset" = -5
  AND "i"."i_category" IN ('Books', 'Music')
  AND "dd"."d_year" = 2001
  AND "dd"."d_moy" = 12
  AND ("p"."p_channel_dmail" = 'Y' OR "p"."p_channel_email" = 'Y' OR "p"."p_channel_tv" = 'Y')
) promotional_sales, (
  SELECT SUM("ss"."ss_ext_sales_price") total
  FROM "SYSTEM"."SPARK_cp_store_sales" "ss"
  JOIN "SYSTEM"."SPARK_date_dim" "dd" ON "ss"."ss_sold_date_sk" = "dd"."d_date_sk"
  JOIN "SYSTEM"."SPARK_cp_item" "i" ON "ss"."ss_item_sk" = "i"."i_item_sk"
  JOIN "SYSTEM"."SPARK_cp_store" "s" ON "ss"."ss_store_sk" = "s"."s_store_sk"
  JOIN "SYSTEM"."SPARK_cp_promotion" "p" ON "ss"."ss_promo_sk" = "p"."p_promo_sk"
  JOIN "SYSTEM"."SPARK_customer" "c" ON "ss"."ss_customer_sk" = "c"."c_customer_sk"
  JOIN "SYSTEM"."SPARK_cp_customer_address" "ca" ON "c"."c_current_addr_sk" = "ca"."ca_address_sk"
  WHERE "ca"."ca_gmt_offset" = -5
  AND "s"."s_gmt_offset" = -5
  AND "i"."i_category" IN ('Books', 'Music')
  AND "dd"."d_year" = 2001
  AND "dd"."d_moy" = 12
) all_sales
-- we don't need a 'ON' join condition. result is just two numbers.
ORDER BY promotions, total
LIMIT 100 -- kinda useless, result is one line with two numbers, but original tpc-ds query has it too.