-- =========================================================
-- SILVER: products_clean (dedupe + idempotência via hash)
-- =========================================================

USE CATALOG workshop_modelagem;
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.products_clean (
  product_id   STRING,
  product_name STRING,
  category     STRING,
  subcategory  STRING,
  brand        STRING,
  cost_price   DECIMAL(18,2),
  list_price   DECIMAL(18,2),
  is_active    STRING,
  last_update  TIMESTAMP,
  row_hash     STRING
) USING DELTA;

-- 1) Stage: normaliza e parseia
CREATE OR REPLACE TEMP VIEW stage_products AS
SELECT
  product_id,
  product_name,
  category,
  subcategory,
  brand,
  CAST(regexp_replace(cost_price, ',', '.') AS DECIMAL(18,2)) AS cost_price,
  CAST(regexp_replace(list_price, ',', '.') AS DECIMAL(18,2)) AS list_price,
  UPPER(CAST(is_active AS STRING)) AS is_active,
  COALESCE(
    try_to_timestamp(last_update, 'yyyy-MM-dd HH:mm:ss'),
    try_to_timestamp(last_update, 'yyyy/MM/dd HH:mm:ss'),
    try_to_timestamp(last_update, 'dd/MM/yyyy HH:mm:ss'),
    try_to_timestamp(last_update, 'dd-MM-yyyy HH:mm:ss'),
    try_to_timestamp(last_update, 'yyyy-MM-dd'),
    try_to_timestamp(last_update, 'yyyy/MM/dd'),
    try_to_timestamp(last_update, 'dd/MM/yyyy'),
    try_to_timestamp(last_update, 'dd-MM-yyyy')
  ) AS parsed_last_update
FROM bronze.products
WHERE product_id IS NOT NULL;

-- 2) Dedup determinístico (última por last_update; empates com ordenação estável)
CREATE OR REPLACE TEMP VIEW stage_products_dedup AS
SELECT
  product_id,
  product_name,
  category,
  subcategory,
  brand,
  cost_price,
  list_price,
  is_active,
  parsed_last_update AS last_update
FROM (
  SELECT
    p.*,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY
        parsed_last_update DESC NULLS LAST,
        product_name DESC,
        category DESC,
        subcategory DESC,
        brand DESC
    ) AS rn
  FROM stage_products p
) z
WHERE rn = 1;

-- 3) Calcula hash de linha (comparação barata e estável)
CREATE OR REPLACE TEMP VIEW stage_products_final AS
SELECT
  product_id,
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
    cast(coalesce(cost_price,   0) as string),
    cast(coalesce(list_price,   0) as string),
    coalesce(is_active,''),
    coalesce(date_format(last_update,'yyyy-MM-dd HH:mm:ss'), '')
  ), 256) AS row_hash
FROM stage_products_dedup;

-- 4) MERGE idempotente (só atualiza quando o hash difere)
MERGE INTO silver.products_clean AS t
USING stage_products_final AS s
ON t.product_id = s.product_id
WHEN MATCHED AND (
  t.row_hash IS NULL OR t.row_hash <> s.row_hash
) THEN UPDATE SET
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
  product_id, product_name, category, subcategory, brand,
  cost_price, list_price, is_active, last_update, row_hash
) VALUES (
  s.product_id, s.product_name, s.category, s.subcategory, s.brand,
  s.cost_price, s.list_price, s.is_active, s.last_update, s.row_hash
);
     
     