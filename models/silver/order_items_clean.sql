-- =========================================================
-- SILVER: order_items_clean (dedupe + idempotência via hash)
-- =========================================================

USE CATALOG workshop_modelagem;
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.order_items_clean (
  order_item_id    STRING,
  order_id         STRING,
  product_id       STRING,
  quantity         DECIMAL(18,2),
  unit_price       DECIMAL(18,2),
  discount_amount  DECIMAL(18,2),
  updated_at       TIMESTAMP,
  row_hash         STRING
) USING DELTA;

-- 1) Stage: normaliza tipos e parseia updated_at com múltiplos formatos
CREATE OR REPLACE TEMP VIEW stage_order_items AS
SELECT
  order_item_id,
  order_id,
  product_id,
  CAST(regexp_replace(quantity, ',', '.') AS DECIMAL(18,2))                    AS quantity,
  CAST(regexp_replace(unit_price, ',', '.') AS DECIMAL(18,2))                  AS unit_price,
  CAST(regexp_replace(COALESCE(discount_amount,'0'), ',', '.') AS DECIMAL(18,2)) AS discount_amount,
  COALESCE(
    try_to_timestamp(updated_at, 'yyyy-MM-dd HH:mm:ss'),
    try_to_timestamp(updated_at, 'yyyy/MM/dd HH:mm:ss'),
    try_to_timestamp(updated_at, 'dd/MM/yyyy HH:mm:ss'),
    try_to_timestamp(updated_at, 'dd-MM-yyyy HH:mm:ss'),
    try_to_timestamp(updated_at, 'yyyy-MM-dd'),
    try_to_timestamp(updated_at, 'yyyy/MM/dd'),
    try_to_timestamp(updated_at, 'dd/MM/yyyy'),
    try_to_timestamp(updated_at, 'dd-MM-yyyy')
  ) AS parsed_updated_at
FROM bronze.order_items
WHERE order_id   IS NOT NULL
  AND product_id IS NOT NULL;

-- 2) Janela incremental (watermark) - só processa últimos 60 dias
CREATE OR REPLACE TEMP VIEW stage_order_items_win AS
SELECT *
FROM stage_order_items
WHERE parsed_updated_at >= date_sub(current_timestamp(), 60);

-- 3) Dedup: mantém 1 linha por (order_id, product_id), a mais recente por updated_at
CREATE OR REPLACE TEMP VIEW stage_order_items_dedup AS
SELECT
  order_item_id,
  order_id,
  product_id,
  quantity,
  unit_price,
  discount_amount,
  parsed_updated_at AS updated_at
FROM (
  SELECT
    s.*,
    ROW_NUMBER() OVER (
      PARTITION BY order_id, product_id
      ORDER BY parsed_updated_at DESC NULLS LAST,
               order_item_id DESC          -- desempate determinístico
    ) AS rn
  FROM stage_order_items_win s
  WHERE parsed_updated_at IS NOT NULL
) z
WHERE rn = 1;

-- 4) Calcula hash da linha para garantir UPDATE só quando mudar
CREATE OR REPLACE TEMP VIEW stage_order_items_final AS
SELECT
  order_item_id,
  order_id,
  product_id,
  quantity,
  unit_price,
  discount_amount,
  updated_at,
  sha2(concat_ws('||',
    cast(coalesce(quantity,0)        as string),
    cast(coalesce(unit_price,0)      as string),
    cast(coalesce(discount_amount,0) as string),
    coalesce(date_format(updated_at,'yyyy-MM-dd HH:mm:ss'),'')
  ), 256) AS row_hash
FROM stage_order_items_dedup;

-- 5) MERGE idempotente: atualiza só quando o hash difere
MERGE INTO silver.order_items_clean AS t
USING stage_order_items_final AS s
ON  t.order_id   = s.order_id
AND t.product_id = s.product_id
WHEN MATCHED AND (t.row_hash IS NULL OR t.row_hash <> s.row_hash) THEN UPDATE SET
  t.order_item_id   = s.order_item_id,
  t.quantity        = s.quantity,
  t.unit_price      = s.unit_price,
  t.discount_amount = s.discount_amount,
  t.updated_at      = s.updated_at,
  t.row_hash        = s.row_hash
WHEN NOT MATCHED THEN INSERT (
  order_item_id, order_id, product_id, quantity, unit_price, discount_amount, updated_at, row_hash
) VALUES (
  s.order_item_id, s.order_id, s.product_id, s.quantity, s.unit_price, s.discount_amount, s.updated_at, s.row_hash
);