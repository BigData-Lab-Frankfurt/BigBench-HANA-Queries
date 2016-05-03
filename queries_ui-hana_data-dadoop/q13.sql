-- based on tpc-ds q74
-- Display customers with both store and web sales in
-- consecutive years for whom the increase in web sales exceeds the increase in
-- store sales for a specified year.

-- Resources
DROP TABLE bigbench_tmp_table1;
CREATE COLUMN TABLE bigbench_tmp_table1 (
  customer_id         VARCHAR(5000),
  customer_first_name VARCHAR(5000),
  customer_last_name  VARCHAR(5000),
  year_total          DECIMAL(15,2),
  year          	  INT,
  sale_type           VARCHAR(5000)
)
--PARTITIONED BY (year INT, sale_type STRING)
--CLUSTERED BY (customer_id)
--SORTED BY (customer_id)
--INTO 16 BUCKETS
--STORED AS ORC
;


--table contains the values of the intersection of customer table and store_sales tables values
--that meet the necessary requirements and whose year value is either 1999 or 2000
INSERT INTO bigbench_tmp_table1
--PARTITION (year, sale_type)
SELECT
  "c"."c_customer_id"    AS customer_id,
  "c"."c_first_name"     AS customer_first_name,
  "c"."c_last_name"      AS customer_last_name,
  SUM("ss"."ss_net_paid") AS year_total,
  "d_year"           AS year,
  's'              AS sale_type
FROM "SYSTEM"."SPARK_customer" "c"
INNER JOIN (
  SELECT "ss"."ss_customer_sk", "ss"."ss_net_paid", "dd"."d_year"
  FROM "SYSTEM"."SPARK_cp_store_sales" "ss"
  JOIN (
    SELECT "d"."d_date_sk", "d"."d_year"
    FROM "SYSTEM"."SPARK_date_dim" "d"
    WHERE "d"."d_year" in (2001, (2001 + 1))
  ) "dd" on ( "ss"."ss_sold_date_sk" = "dd"."d_date_sk" )
) "ss"
ON "c"."c_customer_sk" = "ss"."ss_customer_sk"
GROUP BY
  "c"."c_customer_id",
  "c"."c_first_name",
  "c"."c_last_name",
  "d_year"
;

--table contains the values of the intersection of customer table and web_sales tables values that 
--meet the necessary requirements and whose year value is either 1999 or 2000
INSERT INTO bigbench_tmp_table1
--PARTITION (year, sale_type)
SELECT
  "c"."c_customer_id"    AS customer_id,
  "c"."c_first_name"     AS customer_first_name,
  "c"."c_last_name"      AS customer_last_name,
  SUM("ws"."ws_net_paid") AS year_total,
  "d_year"           AS year,
  'w'              AS sale_type
FROM "SYSTEM"."SPARK_customer" "c"
INNER JOIN (
  SELECT "ws"."ws_bill_customer_sk", "ws"."ws_net_paid", "dd"."d_year"
  FROM "SYSTEM"."SPARK_cp_web_sales" "ws"
  JOIN (
    SELECT "d"."d_date_sk", "d"."d_year"
    FROM "SYSTEM"."SPARK_date_dim" "d"
    WHERE "d"."d_year" in (2001, (2001 + 1) )
  ) "dd" ON ( "ws"."ws_sold_date_sk" = "dd"."d_date_sk" )
) "ws" ON "c"."c_customer_sk" = "ws"."ws_bill_customer_sk"
GROUP BY
  "c"."c_customer_id",
  "c"."c_first_name",
  "c"."c_last_name",
  "d_year"
;

-- the real query part
SELECT
  ts_s.customer_id,
  ts_s.customer_first_name,
  ts_s.customer_last_name,
  CASE WHEN tw_f.year_total > 0
    THEN tw_s.year_total / tw_f.year_total
    ELSE null END,
  CASE WHEN ts_f.year_total > 0
    THEN ts_s.year_total / ts_f.year_total
    ELSE null END
FROM bigbench_tmp_table1 ts_f
  INNER JOIN bigbench_tmp_table1 ts_s ON ts_f.customer_id = ts_s.customer_id
  INNER JOIN bigbench_tmp_table1 tw_f ON ts_f.customer_id = tw_f.customer_id
  INNER JOIN bigbench_tmp_table1 tw_s ON ts_f.customer_id = tw_s.customer_id
WHERE ts_s.customer_id = ts_f.customer_id
AND ts_f.customer_id = tw_s.customer_id
AND ts_f.customer_id = tw_f.customer_id
AND ts_f.sale_type   = 's'
AND tw_f.sale_type   = 'w'
AND ts_s.sale_type   = 's'
AND tw_s.sale_type   = 'w'
AND ts_f.year        = 2001
AND ts_s.year        = (2001 + 1)
AND tw_f.year        = 2001
AND tw_s.year        = (2001 + 1)
AND ts_f.year_total  > 0
AND tw_f.year_total  > 0
ORDER BY
  ts_s.customer_id,
  ts_s.customer_first_name,
  ts_s.customer_last_name
LIMIT 100;

--cleanup -----------------------------------------------------------
DROP TABLE bigbench_tmp_table1;
