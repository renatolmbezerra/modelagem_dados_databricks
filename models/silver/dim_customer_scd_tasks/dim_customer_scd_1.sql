USE CATALOG workshop_modelagem;
CREATE SCHEMA IF NOT EXISTS silver;


-- ============================================================
-- SCD TYPE 2 - Customers (Silver) com HASH + DEDUP incremental
-- ============================================================


-- 1) Tabela alvo (inclui row_hash para idempotência)
CREATE TABLE IF NOT EXISTS silver.dim_customer_scd (
  customer_sk      BIGINT GENERATED ALWAYS AS IDENTITY,
  customer_id      STRING,
  customer_name    STRING,
  email            STRING,
  city             STRING,
  state            STRING,
  effective_start  TIMESTAMP,
  effective_end    TIMESTAMP,
  is_current       BOOLEAN,
  row_hash         STRING
) USING DELTA;


-- (Opcional) Backfill do hash se a tabela já existia sem ele
-- Evita update em massa no primeiro MERGE/INSERT depois de adicionar a coluna
UPDATE silver.dim_customer_scd
SET row_hash = COALESCE(row_hash,
  sha2(concat_ws('||',
    coalesce(customer_name,''),
    coalesce(lower(email),''),              -- normaliza email em lowercase
    coalesce(city,''),
    coalesce(upper(trim(state)),'')         -- normaliza UF
  ), 256)
);


-- 2) Stage bruto: parse robusto do last_update_date -> src_ts
CREATE OR REPLACE TEMP VIEW stage_customers_raw AS
SELECT
  customer_id,
  customer_name,
  lower(email)                    AS email_norm,       -- normaliza e-mail
  city,
  upper(trim(state))              AS state_norm,       -- normaliza UF (SP, RJ, ...)
  COALESCE(
    try_to_timestamp(last_update_date, 'yyyy-MM-dd HH:mm:ss'),
    try_to_timestamp(last_update_date, 'yyyy/MM/dd HH:mm:ss'),
    try_to_timestamp(last_update_date, 'dd/MM/yyyy HH:mm:ss'),
    try_to_timestamp(last_update_date, 'dd-MM-yyyy HH:mm:ss'),
    try_to_timestamp(last_update_date, 'yyyy-MM-dd'),
    try_to_timestamp(last_update_date, 'yyyy/MM/dd'),
    try_to_timestamp(last_update_date, 'dd/MM/yyyy'),
    try_to_timestamp(last_update_date, 'dd-MM-yyyy')
  ) AS src_ts
FROM bronze.customers
WHERE customer_id IS NOT NULL;


-- 3) Janela incremental (watermark de 90 dias)
CREATE OR REPLACE TEMP VIEW stage_customers_window AS
SELECT *
FROM stage_customers_raw
WHERE COALESCE(src_ts, current_timestamp()) >= date_sub(current_timestamp(), 90);


-- 4) Dedup por customer_id (última versão por src_ts; desempate determinístico)
CREATE OR REPLACE TEMP VIEW stage_customers_latest AS
SELECT
  customer_id,
  customer_name,
  email_norm      AS email,
  city,
  state_norm      AS state,
  src_ts
FROM (
  SELECT
    s.*,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY src_ts DESC NULLS LAST,
               customer_name DESC,
               email_norm DESC,
               city DESC,
               state_norm DESC
    ) AS rn
  FROM stage_customers_window s
) z
WHERE rn = 1;


-- 5) Calcula hash da "linha de negócio" (define mudança real)
CREATE OR REPLACE TEMP VIEW stage_customers_hash AS
SELECT
  customer_id,
  customer_name,
  email,
  city,
  state,
  src_ts,
  sha2(concat_ws('||',
    coalesce(customer_name,''),
    coalesce(email,''),
    coalesce(city,''),
    coalesce(state,'')
  ), 256) AS source_hash
FROM stage_customers_latest;


-- 6) Expirar versões correntes QUE mudaram (usa hash para evitar updates desnecessários)
MERGE INTO silver.dim_customer_scd AS tgt
USING stage_customers_hash AS src
ON  tgt.customer_id = src.customer_id
AND tgt.is_current  = TRUE
WHEN MATCHED AND tgt.row_hash <> src.source_hash THEN
  UPDATE SET
    tgt.effective_end = COALESCE(src.src_ts, current_timestamp()),
    tgt.is_current    = FALSE;


-- 7) Inserir primeira versão OU nova versão apenas quando necessário
INSERT INTO silver.dim_customer_scd (
  customer_id, customer_name, email, city, state,
  effective_start, effective_end, is_current, row_hash
)
SELECT
  s.customer_id,
  s.customer_name,
  s.email,
  s.city,
  s.state,
  COALESCE(s.src_ts, current_timestamp()) AS effective_start,
  TIMESTAMP('9999-12-31')                 AS effective_end,
  TRUE                                    AS is_current,
  s.source_hash                           AS row_hash
FROM stage_customers_hash s
LEFT JOIN silver.dim_customer_scd c
  ON c.customer_id = s.customer_id AND c.is_current = TRUE
WHERE c.customer_id IS NULL           -- novo cliente
   OR c.row_hash <> s.source_hash;    -- mudança real