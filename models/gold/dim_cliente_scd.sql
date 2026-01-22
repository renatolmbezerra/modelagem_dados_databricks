-- ============================================
-- GOLD: dim_cliente_scd (replicação da SCD da Silver)
-- ============================================
USE CATALOG workshop_modelagem;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.dim_cliente_scd (
  customer_sk      BIGINT,
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

-- Stage com hash para idempotência
CREATE OR REPLACE TEMP VIEW dim_cliente_scd_stage AS
SELECT
  s.customer_sk,
  s.customer_id,
  s.customer_name,
  s.email,
  s.city,
  s.state,
  s.effective_start,
  s.effective_end,
  s.is_current,
  sha2(concat_ws('||',
    coalesce(s.customer_name,''),
    coalesce(lower(s.email),''),
    coalesce(trim(s.city),''),
    coalesce(upper(trim(s.state)),''),
    coalesce(date_format(s.effective_start,'yyyy-MM-dd HH:mm:ss'),''),
    coalesce(date_format(s.effective_end,  'yyyy-MM-dd HH:mm:ss'),''),
    cast(coalesce(s.is_current,false) as string)
  ),256) AS row_hash
FROM silver.dim_customer_scd s;

MERGE INTO gold.dim_cliente_scd t
USING dim_cliente_scd_stage s
ON t.customer_sk = s.customer_sk
WHEN MATCHED AND (t.row_hash IS NULL OR t.row_hash <> s.row_hash) THEN UPDATE SET
  t.customer_id      = s.customer_id,
  t.customer_name    = s.customer_name,
  t.email            = s.email,
  t.city             = s.city,
  t.state            = s.state,
  t.effective_start  = s.effective_start,
  t.effective_end    = s.effective_end,
  t.is_current       = s.is_current,
  t.row_hash         = s.row_hash
WHEN NOT MATCHED THEN INSERT (
  customer_sk, customer_id, customer_name, email, city, state,
  effective_start, effective_end, is_current, row_hash
) VALUES (
  s.customer_sk, s.customer_id, s.customer_name, s.email, s.city, s.state,
  s.effective_start, s.effective_end, s.is_current, s.row_hash
);