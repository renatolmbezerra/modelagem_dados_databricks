-- ============================================================
-- GOLD: dim_cliente (snapshot corrente, idempotente)
-- ============================================================

USE CATALOG workshop_modelagem;
CREATE SCHEMA IF NOT EXISTS gold;

-- 1) Tabela alvo (1 linha por NK; com row_hash para idempotência)
CREATE TABLE IF NOT EXISTS gold.dim_cliente (
  customer_sk      BIGINT,          -- SK atual da SCD (muda quando abre nova versão)
  nk_customer_id   STRING,          -- NK estável do cliente (chave do MERGE)
  customer_name    STRING,
  email            STRING,
  city             STRING,
  state            STRING,
  effective_start  TIMESTAMP,
  effective_end    TIMESTAMP,
  row_hash         STRING
) USING DELTA;

-- Backfill do hash se necessário (evita updates "em massa" na 1ª execução após adicionar a coluna)
UPDATE gold.dim_cliente
SET row_hash = COALESCE(row_hash,
  sha2(concat_ws('||',
    coalesce(customer_name,''),
    coalesce(lower(email),''),
    coalesce(trim(city),''),
    coalesce(upper(trim(state)),''),
    coalesce(date_format(effective_start,'yyyy-MM-dd HH:mm:ss'),''),
    coalesce(date_format(effective_end,  'yyyy-MM-dd HH:mm:ss'),'')
  ),256)
);

-- 2) Stage: pega SOMENTE o corrente da SCD, normaliza e calcula hash
CREATE OR REPLACE TEMP VIEW dim_cliente_stage AS
SELECT
  s.customer_sk,
  s.customer_id                                      AS nk_customer_id,
  s.customer_name,
  lower(s.email)                                     AS email,
  trim(s.city)                                       AS city,
  upper(trim(s.state))                               AS state,
  s.effective_start,
  s.effective_end,
  sha2(concat_ws('||',
    coalesce(s.customer_name,''),
    coalesce(lower(s.email),''),
    coalesce(trim(s.city),''),
    coalesce(upper(trim(s.state)),''),
    coalesce(date_format(s.effective_start,'yyyy-MM-dd HH:mm:ss'),''),
    coalesce(date_format(s.effective_end,  'yyyy-MM-dd HH:mm:ss'),'')
  ),256) AS row_hash
FROM silver.dim_customer_scd s
WHERE s.is_current = TRUE;

-- 3) MERGE idempotente (1 linha por nk_customer_id)
MERGE INTO gold.dim_cliente t
USING dim_cliente_stage s
ON t.nk_customer_id = s.nk_customer_id                 -- snapshot corrente = 1 por NK
WHEN MATCHED AND (t.row_hash IS NULL OR t.row_hash <> s.row_hash) THEN
  UPDATE SET
    t.customer_sk     = s.customer_sk,                 -- atualiza SK quando a SCD virar
    t.customer_name   = s.customer_name,
    t.email           = s.email,
    t.city            = s.city,
    t.state           = s.state,
    t.effective_start = s.effective_start,
    t.effective_end   = s.effective_end,
    t.row_hash        = s.row_hash
WHEN NOT MATCHED THEN INSERT (
  customer_sk, nk_customer_id, customer_name, email, city, state, effective_start, effective_end, row_hash
) VALUES (
  s.customer_sk, s.nk_customer_id, s.customer_name, s.email, s.city, s.state, s.effective_start, s.effective_end, s.row_hash
);

     