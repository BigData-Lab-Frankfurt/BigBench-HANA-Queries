-- based on tpc-ds q90
-- What is the ratio between the number of items sold over
-- the internet in the morning (7 to 8am) to the number of items sold in the evening
-- (7 to 8pm) of customers with a specified number of dependents. Consider only
-- websites with a high amount of content.

-- Resources
-- the real query part
SELECT CAST(amc as double) / CAST(pmc as double) am_pm_ratio
FROM (
  SELECT COUNT(*) amc
  FROM import_cp_web_sales ws
  JOIN import_household_demographics hd ON hd.hd_demo_sk = ws.ws_ship_hdemo_sk
  AND hd.hd_dep_count = 5
  JOIN import_time_dim td ON td.t_time_sk = ws.ws_sold_time_sk
  AND td.t_hour >= 7
  AND td.t_hour <= 8
  JOIN import_web_page wp ON wp.wp_web_page_sk = ws.ws_web_page_sk
  AND wp.wp_char_count >= 5000
  AND wp.wp_char_count <= 6000
) at, (
  SELECT COUNT(*) pmc
  FROM import_cp_web_sales ws
  JOIN import_household_demographics hd ON ws.ws_ship_hdemo_sk = hd.hd_demo_sk
  AND hd.hd_dep_count = 5
  JOIN import_time_dim td ON td.t_time_sk = ws.ws_sold_time_sk
  AND td.t_hour >= 19
  AND td.t_hour <= 20
  JOIN import_web_page wp ON wp.wp_web_page_sk = ws.ws_web_page_sk
  AND wp.wp_char_count >= 5000
  AND wp.wp_char_count <= 6000
) pt
--result is a single line
;
