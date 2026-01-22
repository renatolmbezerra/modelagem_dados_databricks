-- =========================================================
-- SILVER: orders_clean (dedupe + idempotência via hash)
-- =========================================================

USE CATALOG workshop_modelagem;
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.orders_clean (
  order_id      STRING,
  customer_id   STRING,
  order_date    DATE,
  order_status  STRING,
  total_amount  DECIMAL(18,2),
  row_hash      STRING
) USING DELTA;

-- 1) Stage: normaliza e parseia order_date para timestamp (order_ts) + DATE
CREATE OR REPLACE TEMP VIEW stage_orders AS
SELECT
  order_id,
  customer_id,
  -- timestamp de referência (suporta múltiplos formatos)
  COALESCE(
    try_to_timestamp(order_date, 'yyyy-MM-dd HH:mm:ss'),
    try_to_timestamp(order_date, 'yyyy/MM/dd HH:mm:ss'),
    try_to_timestamp(order_date, 'dd/MM/yyyy HH:mm:ss'),
    try_to_timestamp(order_date, 'dd-MM-yyyy HH:mm:ss'),
    try_to_timestamp(order_date, 'yyyy-MM-dd'),
    try_to_timestamp(order_date, 'yyyy/MM/dd'),
    try_to_timestamp(order_date, 'dd/MM/yyyy'),
    try_to_timestamp(order_date, 'dd-MM-yyyy')
  ) AS order_ts,
  UPPER(TRIM(order_status)) AS order_status_norm,
  CAST(regexp_replace(total_amount, ',', '.') AS DECIMAL(18,2)) AS total_amount_norm
FROM bronze.orders
WHERE order_id IS NOT NULL;

-- 2) Janela incremental (watermark de 60 dias)
CREATE OR REPLACE TEMP VIEW stage_orders_win AS
SELECT *
FROM stage_orders
WHERE order_ts >= date_sub(current_timestamp(), 60);

-- 3) Dedup: mantém 1 linha por order_id (mais recente por order_ts)
CREATE OR REPLACE TEMP VIEW stage_orders_dedup AS
SELECT
  order_id,
  customer_id,
  CAST(order_ts AS DATE)          AS order_date,
  order_status_norm               AS order_status,
  total_amount_norm               AS total_amount,
  order_ts
FROM (
  SELECT
    s.*,
    ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY order_ts DESC NULLS LAST,
               customer_id DESC            -- desempate determinístico
    ) AS rn
  FROM stage_orders_win s
  WHERE order_ts IS NOT NULL
) z
WHERE rn = 1;

-- 4) Calcula hash para idempotência (evita UPDATE sem mudança real)
CREATE OR REPLACE TEMP VIEW stage_orders_final AS
SELECT
  order_id,
  customer_id,
  order_date,
  order_status,
  total_amount,
  sha2(concat_ws('||',
    coalesce(customer_id,''),
    coalesce(date_format(order_date,'yyyy-MM-dd'),''),
    coalesce(order_status,''),
    cast(coalesce(total_amount,0) as string)
  ), 256) AS row_hash
FROM stage_orders_dedup;

-- 5) MERGE idempotente: só atualiza quando o hash difere
MERGE INTO silver.orders_clean AS t
USING stage_orders_final AS s
ON t.order_id = s.order_id
WHEN MATCHED AND (t.row_hash IS NULL OR t.row_hash <> s.row_hash) THEN UPDATE SET
  t.customer_id  = s.customer_id,
  t.order_date   = s.order_date,
  t.order_status = s.order_status,
  t.total_amount = s.total_amount,
  t.row_hash     = s.row_hash
WHEN NOT MATCHED THEN INSERT (order_id, customer_id, order_date, order_status, total_amount, row_hash)
VALUES (s.order_id, s.customer_id, s.order_date, s.order_status, s.total_amount, s.row_hash);
     