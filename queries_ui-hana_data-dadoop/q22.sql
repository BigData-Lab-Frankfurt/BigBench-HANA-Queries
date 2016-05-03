-- based on tpc-ds q21
-- For all items whose price was changed on a given date,
-- compute the percentage change in inventory between the 30-day period BEFORE
-- the price change and the 30-day period AFTER the change. Group this
-- information by warehouse.

-- Resources
-- the real query part
SELECT
  "w"."w_warehouse_name",
  "i"."i_item_id",
  
  SUM( CASE WHEN 
  	(LEFT("d"."d_date",4) = 2001 AND (DAYOFYEAR('2001-05-08') - DAYOFYEAR("d"."d_date")) >= 0)
  	OR
  	(((DAYOFYEAR('2001-05-08') - DAYOFYEAR("d"."d_date") ) + (2001 - LEFT("d"."d_date",4)) * 365) >= 0)
    THEN "inv"."inv_quantity_on_hand"
    ELSE 0 END
  ) AS inv_before,  
    SUM( CASE WHEN 
  	(LEFT("d"."d_date",4) = 2001 AND (DAYOFYEAR('2001-05-08') - DAYOFYEAR("d"."d_date")) < 0)
  	OR
  	(((DAYOFYEAR('2001-05-08') - DAYOFYEAR("d"."d_date") ) + (2001 - LEFT("d"."d_date",4)) * 365) < 0)
    THEN "inv"."inv_quantity_on_hand"
    ELSE 0 END
  ) AS inv_after  
FROM "SYSTEM"."SPARK_inventory" "inv",
  "SYSTEM"."SPARK_cp_item" "i",
  "SYSTEM"."SPARK_cp_warehouse" "w",
  "SYSTEM"."SPARK_date_dim" "d"
WHERE "i"."i_current_price" BETWEEN 0.98 AND 1.5
AND "i"."i_item_sk"        = "inv"."inv_item_sk"
AND "inv"."inv_warehouse_sk" = "w"."w_warehouse_sk"
AND "inv"."inv_date_sk"      = "d"."d_date_sk"
AND (
		(LEFT("d"."d_date",4) = 2001 AND (DAYOFYEAR('2001-05-08') - DAYOFYEAR("d"."d_date")) >= -30)
  	OR
  		( ( (DAYOFYEAR('2001-05-08') - DAYOFYEAR("d"."d_date")) + ((2001 - LEFT("d"."d_date",4)) * 365) ) >= -30 )
  	)
AND (
		(LEFT("d"."d_date",4) = 2001 AND (DAYOFYEAR('2001-05-08') - DAYOFYEAR("d"."d_date")) <= 30)
  	OR
  		( ( (DAYOFYEAR('2001-05-08') - DAYOFYEAR("d"."d_date")) + ((2001 - LEFT("d"."d_date",4)) * 365) ) <= 30 )
  	)
GROUP BY "w"."w_warehouse_name", "i"."i_item_id"
HAVING SUM( CASE WHEN 
  	(LEFT("d"."d_date",4) = 2001 AND (DAYOFYEAR('2001-05-08') - DAYOFYEAR("d"."d_date")) < 0)
  	OR
  	(((DAYOFYEAR('2001-05-08') - DAYOFYEAR("d"."d_date") ) + (2001 - LEFT("d"."d_date",4)) * 365) < 0)
    THEN "inv"."inv_quantity_on_hand"
    ELSE 0 END ) > 0
AND SUM( CASE WHEN 
  	(LEFT("d"."d_date",4) = 2001 AND (DAYOFYEAR('2001-05-08') - DAYOFYEAR("d"."d_date")) >= 0)
  	OR
  	(((DAYOFYEAR('2001-05-08') - DAYOFYEAR("d"."d_date") ) + (2001 - LEFT("d"."d_date",4)) * 365) >= 0)
    THEN "inv"."inv_quantity_on_hand"
    ELSE 0 END
  ) / SUM( CASE WHEN 
  	(LEFT("d"."d_date",4) = 2001 AND (DAYOFYEAR('2001-05-08') - DAYOFYEAR("d"."d_date")) < 0)
  	OR
  	(((DAYOFYEAR('2001-05-08') - DAYOFYEAR("d"."d_date") ) + (2001 - LEFT("d"."d_date",4)) * 365) < 0)
    THEN "inv"."inv_quantity_on_hand"
    ELSE 0 END
  ) >= 2.0 / 3.0  
AND SUM( CASE WHEN 
  	(LEFT("d"."d_date",4) = 2001 AND (DAYOFYEAR('2001-05-08') - DAYOFYEAR("d"."d_date")) >= 0)
  	OR
  	(((DAYOFYEAR('2001-05-08') - DAYOFYEAR("d"."d_date") ) + (2001 - LEFT("d"."d_date",4)) * 365) >= 0)
    THEN "inv"."inv_quantity_on_hand"
    ELSE 0 END
  ) / SUM( CASE WHEN 
  	(LEFT("d"."d_date",4) = 2001 AND (DAYOFYEAR('2001-05-08') - DAYOFYEAR("d"."d_date")) < 0)
  	OR
  	(((DAYOFYEAR('2001-05-08') - DAYOFYEAR("d"."d_date") ) + (2001 - LEFT("d"."d_date",4)) * 365) < 0)
    THEN "inv"."inv_quantity_on_hand"
    ELSE 0 END
  ) <= 3.0 / 2.0
ORDER BY "w"."w_warehouse_name", "i"."i_item_id"
LIMIT 100
;






