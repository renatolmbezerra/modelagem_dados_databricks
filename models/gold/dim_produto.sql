-- ============================================
-- GOLD: dim_produto (NK = product_id)
-- ============================================

USE CATALOG workshop_modelagem;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.dim_produto (
  product_sk    BIGINT GENERATED ALWAYS AS IDENTITY,
  nk_product_id STRING,
  product_name  STRING,
  category      STRING,
  subcategory   STRING,
  brand         STRING,
  cost_price    DECIMAL(18,2),
  list_price    DECIMAL(18,2),
  is_active     STRING,
  last_update   TIMESTAMP,
  row_hash      STRING
) USING DELTA;

-- Stage com hash (reuso da Silver já “clean”)
CREATE OR REPLACE TEMP VIEW dim_produto_stage AS
SELECT
  product_id      AS nk_product_id,
  product_name,
  category,
  subcategory,
  brand,
  cost_price,
  list_price,
  is_active,
  last_update,
  sha2(concat_ws('||',
    coalesce(product_name,''),
    coalesce(category,''),
    coalesce(subcategory,''),
    coalesce(brand,''),
    cast(coalesce(cost_price,0) as string),
    cast(coalesce(list_price,0) as string),
    coalesce(is_active,''),
    coalesce(date_format(last_update,'yyyy-MM-dd HH:mm:ss'),'')
  ),256) AS row_hash
FROM silver.products_clean;

MERGE INTO gold.dim_produto t
USING dim_produto_stage s
ON t.nk_product_id = s.nk_product_id
WHEN MATCHED AND (t.row_hash IS NULL OR t.row_hash <> s.row_hash) THEN UPDATE SET
  t.product_name = s.product_name,
  t.category     = s.category,
  t.subcategory  = s.subcategory,
  t.brand        = s.brand,
  t.cost_price   = s.cost_price,
  t.list_price   = s.list_price,
  t.is_active    = s.is_active,
  t.last_update  = s.last_update,
  t.row_hash     = s.row_hash
WHEN NOT MATCHED THEN INSERT (
  nk_product_id, product_name, category, subcategory, brand,
  cost_price, list_price, is_active, last_update, row_hash
) VALUES (
  s.nk_product_id, s.product_name, s.category, s.subcategory, s.brand,
  s.cost_price, s.list_price, s.is_active, s.last_update, s.row_hash
);

     