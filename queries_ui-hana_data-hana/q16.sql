-- based on tpc-ds q40
-- Compute the impact of an item price change on the
-- store sales by computing the total sales for items in a 30 day period before and
-- after the price change. Group the items by location of warehouse where they
-- were delivered from.

-- Resources
-- the real query part
SELECT w_state, i_item_id,
  SUM(
    CASE WHEN (TO_DATE(d.d_date, 'YYYY-MM-DD') < TO_DATE('2001-03-16', 'YYYY-MM-DD'))
    THEN ws_sales_price - COALESCE(wr_refunded_cash,0)
    ELSE 0.0 END
  ) AS sales_before,
  SUM(
    CASE WHEN (TO_DATE(d.d_date, 'YYYY-MM-DD') >= TO_DATE('2001-03-16', 'YYYY-MM-DD'))
    THEN ws_sales_price - COALESCE(wr_refunded_cash,0)
    ELSE 0.0 END
  ) AS sales_after
FROM (
  SELECT *
  FROM import_cp_web_sales ws
  LEFT OUTER JOIN import_cp_web_returns wr ON (ws.ws_order_number = wr.wr_order_number
    AND ws.ws_item_sk = wr.wr_item_sk)
) a1
JOIN import_cp_item i ON a1.ws_item_sk = i.i_item_sk
JOIN import_cp_warehouse w ON a1.ws_warehouse_sk = w.w_warehouse_sk
JOIN import_date_dim d ON a1.ws_sold_date_sk = d.d_date_sk
AND TO_DATE(d.d_date, 'YYYY-MM-DD') >= ADD_DAYS(TO_DATE('2001-03-16', 'YYYY-MM-DD'), -30) --subtract 30 days in seconds
AND TO_DATE(d.d_date, 'YYYY-MM-DD') <= ADD_DAYS(TO_DATE('2001-03-16', 'YYYY-MM-DD'), 30) --add 30 days in seconds
GROUP BY w_state,i_item_id
--original was ORDER BY w_state,i_item_id , but CLUSTER BY is hives cluster scale counter part
ORDER BY w_state,i_item_id
LIMIT 100
;