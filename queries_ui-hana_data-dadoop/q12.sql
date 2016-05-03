-- Find all customers who viewed items of a given category on the web
-- in a given month and year that was followed by an in-store purchase of an item from the same category in the three
-- consecutive months.

-- Resources
-- the real query part
SELECT DISTINCT "wcs_user_sk" -- Find all customers
-- TODO check if 37134 is first day of the month
FROM
( -- web_clicks viewed items in date range with items from specified categories
  SELECT
    "wcs_user_sk",
    "wcs_click_date_sk"
  FROM "SYSTEM"."SPARK_web_clickstreams", "SYSTEM"."SPARK_cp_item"
  WHERE "wcs_click_date_sk" BETWEEN 37134 AND (37134 + 30) -- in a given month and year
  AND "i_category" IN ('Books', 'Electronics') -- filter given category
  AND "wcs_item_sk" = "i_item_sk"
  AND "wcs_user_sk" IS NOT NULL
  AND "wcs_sales_sk" IS NULL --only views, not purchases
  ) webInRange,
(  -- store sales in date range with items from specified categories
  SELECT
    "ss_customer_sk",
    "ss_sold_date_sk"
  FROM "SYSTEM"."SPARK_cp_store_sales", "SYSTEM"."SPARK_cp_item"
  WHERE "ss_sold_date_sk" BETWEEN 37134 AND (37134 + 90) -- in the three consecutive months.
  AND "i_category" IN ('Books', 'Electronics') -- filter given category 
  AND "ss_item_sk" = "i_item_sk"
  AND "ss_customer_sk" IS NOT NULL
) storeInRange
-- join web and store
WHERE "wcs_user_sk" = "ss_customer_sk"
AND "wcs_click_date_sk" < "ss_sold_date_sk" -- buy AFTER viewed on website
ORDER BY "wcs_user_sk"
--CLUSTER BY instead of ORDER BY does not work to achieve global ordering. e.g. 2 reducers: first reducer will write keys 0,2,4,6.. into file 000000_0 and reducer 2 will write keys 1,3,5,7,.. into file 000000_1.concatenating these files does not produces a deterministic result if number of reducer changes.
--Solution: parallel "order by" as non parallel version only uses a single reducer and we cant use "limit"
;
