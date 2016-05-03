-- For a given product, measure the effect of competitor's prices on
-- products' in-store and online sales. (Compute the cross-price elasticity of demand
-- for a given product.
-- IMPLEMENTATION NOTICE:
-- Step1 :
-- Calculating the Percentage Change in Quantity Demanded of Good X : [QDemand(NEW) - QDemand(OLD)] / QDemand(OLD)
-- Step 2:
-- Calculating the Percentage Change in Price of Good Y: [Price(NEW) - Price(OLD)] / Price(OLD)
-- Step 3 final:
-- Cross-Price Elasticity of Demand (CPEoD) is given by: CPEoD = (% Change in Quantity Demand for Good X)/(% Change in Price for Good Y))

-- Resources
-- compute the price change % for the competitor times
-- Resources
-- compute the price change % for the competitor times
DROP VIEW bigbench_tmp_view1;
CREATE VIEW bigbench_tmp_view1 AS
SELECT
  "i"."i_item_sk",
  ("imp"."imp_competitor_price" - "i"."i_current_price")/"i"."i_current_price" AS price_change,
  "imp"."imp_start_date",
  ("imp"."imp_end_date" - "imp"."imp_start_date") AS no_days_comp_price
FROM "SYSTEM"."SPARK_cp_item" "i"
JOIN "SYSTEM"."SPARK_cp_item_marketprices" "imp" ON "i"."i_item_sk" = "imp"."imp_item_sk"
WHERE "i"."i_item_sk" IN (10000, 10001)
AND "imp"."imp_competitor_price" < "i"."i_current_price"
ORDER BY "i"."i_item_sk", "imp"."imp_start_date"
;

--websales items sold quantity before and after competitor price change
DROP VIEW bigbench_tmp_view2;
CREATE VIEW bigbench_tmp_view2 AS
SELECT
  "ws"."ws_item_sk",
  SUM(
    CASE WHEN "ws"."ws_sold_date_sk" >= "c"."imp_start_date"
    AND "ws"."ws_sold_date_sk" < "c"."imp_start_date" + "c".no_days_comp_price
    THEN "ws"."ws_quantity"
    ELSE 0 END
  ) AS current_ws_quant,
  SUM(
    CASE WHEN "ws"."ws_sold_date_sk" >= "c"."imp_start_date" - "c".no_days_comp_price
    AND "ws"."ws_sold_date_sk" < "c"."imp_start_date"
    THEN "ws"."ws_quantity"
    ELSE 0 END
  ) AS prev_ws_quant
FROM "SYSTEM"."SPARK_cp_web_sales" "ws"
JOIN "SYSTEM"."BIGBENCH_TMP_VIEW1" "c" ON "ws"."ws_item_sk" = "c"."i_item_sk"
GROUP BY "ws"."ws_item_sk"
;


--storesales items sold quantity before and after competitor price change
DROP VIEW bigbench_tmp_view3;
CREATE VIEW bigbench_tmp_view3 AS
SELECT
  "ss"."ss_item_sk",
  SUM(
    CASE WHEN "ss"."ss_sold_date_sk" >= "c"."imp_start_date"
    AND "ss"."ss_sold_date_sk" < "c"."imp_start_date" + "c".no_days_comp_price
    THEN "ss"."ss_quantity"
    ELSE 0 END
  ) AS current_ss_quant,
  SUM(
    CASE WHEN "ss"."ss_sold_date_sk" >= "c"."imp_start_date" - "c".no_days_comp_price
    AND "ss"."ss_sold_date_sk" < "c"."imp_start_date"
    THEN "ss"."ss_quantity"
    ELSE 0 END
  ) AS prev_ss_quant
FROM "SYSTEM"."SPARK_cp_store_sales" "ss"
JOIN "SYSTEM"."BIGBENCH_TMP_VIEW1" "c" ON "c"."i_item_sk" = "ss"."ss_item_sk"
GROUP BY "ss"."ss_item_sk"
;

-- Begin: the real query part
SELECT
  "c"."i_item_sk",
  ("ss".current_ss_quant + "ws".current_ws_quant - "ss".prev_ss_quant - "ws".prev_ws_quant) / (("ss".prev_ss_quant + "ws".prev_ws_quant) * "c".price_change) AS cross_price_elasticity
FROM "SYSTEM"."BIGBENCH_TMP_VIEW1" "c"
JOIN "SYSTEM"."BIGBENCH_TMP_VIEW2" "ws" ON "c"."i_item_sk" = "ws"."ws_item_sk"
JOIN "SYSTEM"."BIGBENCH_TMP_VIEW3" "ss" ON "c"."i_item_sk" = "ss"."ss_item_sk"
-- no ORDER BY required, result is a single row for the selected item
;

-- clean up -----------------------------------
DROP VIEW bigbench_tmp_view1;
DROP VIEW bigbench_tmp_view2;
DROP VIEW bigbench_tmp_view3;