--- based on tpc-ds q39
-- This query contains multiple, related iterations:
-- Iteration 1: Calculate the coefficient of variation and mean of every item
-- and warehouse of the given and the consecutive month
-- Iteration 2: Find items that had a coefficient of variation of 1.5 or larger
-- in the given and the consecutive month

DROP TABLE bigbench_tmp_table1;
CREATE COLUMN TABLE bigbench_tmp_table1 AS
(SELECT
  w_warehouse_name,
  w_warehouse_sk,
  i_item_sk,
  d_moy,
  stdev,
  mean,
  cast( (CASE mean WHEN 0.0 THEN NULL ELSE stdev/mean END) as decimal(15,5)) cov
FROM (
  SELECT
    w_warehouse_name,
    w_warehouse_sk,
    i_item_sk,
    d_moy,
    cast(stddev_samp(inv_quantity_on_hand) as decimal(15,5)) stdev,
    cast(avg(inv_quantity_on_hand) as decimal(15,5)) mean
  FROM import_inventory inv
  JOIN import_date_dim d ON (inv.inv_date_sk = d.d_date_sk
  AND d.d_year = 2001 )
  JOIN import_cp_item i ON inv.inv_item_sk = i.i_item_sk
  JOIN import_cp_warehouse w ON inv.inv_warehouse_sk = w.w_warehouse_sk
  GROUP BY
    w_warehouse_name,
    w_warehouse_sk,
    i_item_sk,
    d_moy
) q23_tmp_inv_part
WHERE stdev > mean AND mean > 0 --if stdev > mean then stdev/mean is greater 1.0
);

-- Begin: the real query part
SELECT
  inv1.w_warehouse_sk AS inv1_w_warehouse_sk,
  inv1.i_item_sk AS inv1_i_item_sk,
  inv1.d_moy AS inv1_d_moy,
  inv1.mean AS inv1_mean,
  inv1.cov AS inv1_cov,
  inv2.w_warehouse_sk AS inv2_w_warehouse_sk,
  inv2.i_item_sk AS inv2_i_item_sk,
  inv2.d_moy AS inv2_d_moy,
  inv2.mean AS inv2_mean,
  inv2.cov AS inv2_cov
FROM bigbench_tmp_table1 inv1
JOIN bigbench_tmp_table1 inv2 ON (
  inv1.i_item_sk = inv2.i_item_sk
  AND inv1.w_warehouse_sk = inv2.w_warehouse_sk
  AND inv1.d_moy = 1
  AND inv2.d_moy = 1 + 1
)
ORDER BY
  inv1_w_warehouse_sk,
  inv1_i_item_sk,
  inv1_d_moy,
  inv1_mean,
  inv1_cov,
  inv2_d_moy,
  inv2_mean,
  inv2_cov
;

-- Begin: the real query part
SELECT
  inv1.w_warehouse_sk AS inv1_w_warehouse_sk,
  inv1.i_item_sk AS inv1_i_item_sk,
  inv1.d_moy AS inv1_d_moy,
  inv1.mean AS inv1_mean,
  inv1.cov AS inv1_cov,
  inv2.w_warehouse_sk AS inv2_w_warehouse_sk,
  inv2.i_item_sk AS inv2_i_item_sk,
  inv2.d_moy AS inv2_d_moy,
  inv2.mean AS inv2_mean,
  inv2.cov AS inv2_cov
FROM bigbench_tmp_table1 inv1
JOIN bigbench_tmp_table1 inv2 ON (
  inv1.i_item_sk = inv2.i_item_sk
  AND inv1.w_warehouse_sk = inv2.w_warehouse_sk
  AND inv1.d_moy = 1
  AND inv2.d_moy = 1 + 1
  AND inv1.cov > 1.5
)
ORDER BY
  inv1_w_warehouse_sk,
  inv1_i_item_sk,
  inv1_d_moy,
  inv1_mean,
  inv1_cov,
  inv2_d_moy,
  inv2_mean,
  inv2_cov
;

DROP TABLE bigbench_tmp_table1;