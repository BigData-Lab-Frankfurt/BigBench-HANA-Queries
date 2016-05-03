-- based on tpc-ds q21
-- For all items whose price was changed on a given date,
-- compute the percentage change in inventory between the 30-day period BEFORE
-- the price change and the 30-day period AFTER the change. Group this
-- information by warehouse.

-- Resources
-- the real query part
SELECT
  w_warehouse_name,
  i_item_id,
  SUM( CASE WHEN DAYS_BETWEEN (d_date, '2001-05-08') >= 0
    THEN inv_quantity_on_hand
    ELSE 0 END
  ) AS inv_after,
    SUM( CASE WHEN DAYS_BETWEEN (d_date, '2001-05-08') < 0
    THEN inv_quantity_on_hand
    ELSE 0 END
  ) AS inv_before
FROM import_inventory inv,
  import_cp_item i,
  import_cp_warehouse w,
  import_date_dim d
WHERE i_current_price BETWEEN 0.98 AND 1.5
AND i_item_sk        = inv_item_sk
AND inv_warehouse_sk = w_warehouse_sk
AND inv_date_sk      = d_date_sk
AND DAYS_BETWEEN (d_date, '2001-05-08') >= -30
AND DAYS_BETWEEN (d_date, '2001-05-08') <= 30
GROUP BY w_warehouse_name, i_item_id
HAVING SUM( CASE WHEN DAYS_BETWEEN (d_date, '2001-05-08') < 0
    THEN inv_quantity_on_hand
    ELSE 0 END ) > 0
AND SUM( CASE WHEN DAYS_BETWEEN (d_date, '2001-05-08') >= 0
    THEN inv_quantity_on_hand
    ELSE 0 END
  ) / SUM( CASE WHEN DAYS_BETWEEN (d_date, '2001-05-08') < 0
    THEN inv_quantity_on_hand
    ELSE 0 END
  ) >= 2.0 / 3.0
AND SUM( CASE WHEN DAYS_BETWEEN (d_date, '2001-05-08') >= 0
    THEN inv_quantity_on_hand
    ELSE 0 END
  ) / SUM( CASE WHEN DAYS_BETWEEN (d_date, '2001-05-08') < 0
    THEN inv_quantity_on_hand
    ELSE 0 END
  ) <= 3.0 / 2.0
ORDER BY w_warehouse_name, i_item_id
LIMIT 100
;