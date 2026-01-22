-- ============================================
-- GOLD: fact_vendas (grão = item de pedido)
-- Chaves via dimensões GOLD (inclui cliente SCD time-aware)
-- ============================================

USE CATALOG workshop_modelagem;
CREATE SCHEMA IF NOT EXISTS gold;

-- Tabela alvo (particionada por tempo)
CREATE TABLE IF NOT EXISTS gold.fact_vendas (
  order_id        STRING,
  order_item_id   STRING,
  fk_tempo_sk     INT,
  fk_customer_sk  BIGINT,
  fk_product_sk   BIGINT,
  quantity        DECIMAL(18,2),
  unit_price      DECIMAL(18,2),
  discount_amount DECIMAL(18,2),
  revenue_gross   DECIMAL(18,2),  -- quantity * unit_price
  revenue_net     DECIMAL(18,2),  -- revenue_gross - discount_amount
  row_hash        STRING
) USING DELTA
PARTITIONED BY (fk_tempo_sk);

-- =========================
-- Stage (60d de watermark)
-- =========================
CREATE OR REPLACE TEMP VIEW fact_stage AS
WITH
oi AS (   -- Itens do pedido (Silver curado)
  SELECT
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    discount_amount,
    updated_at
  FROM silver.order_items_clean
),
o AS (    -- Pedidos (Silver curado)
  SELECT
    order_id,
    customer_id,
    order_date,
    order_status,
    total_amount
  FROM silver.orders_clean
),
dt AS (   -- Dim tempo (Gold)
  SELECT tempo_sk, data
  FROM gold.dim_tempo
),
dp AS (   -- Dim produto (Gold)
  SELECT nk_product_id, product_sk
  FROM gold.dim_produto
),
c_scd AS ( -- Dim cliente SCD (Gold) para range join
  SELECT
    customer_sk,
    customer_id,
    CAST(effective_start AS DATE) AS eff_start_date,
    CAST(effective_end   AS DATE) AS eff_end_date
  FROM gold.dim_cliente_scd
),
base AS (
  SELECT
    oi.order_item_id,
    oi.order_id,
    o.customer_id,
    o.order_date,
    oi.product_id,
    oi.quantity,
    oi.unit_price,
    oi.discount_amount,
    -- lookups de dimensões (apenas GOLD)
    d.tempo_sk              AS fk_tempo_sk,
    p.product_sk            AS fk_product_sk,
    COALESCE(c.customer_sk, 0) AS fk_customer_sk  -- fallback unknown
  FROM oi
  JOIN o  ON o.order_id = oi.order_id
  JOIN dt d ON d.data   = o.order_date
  LEFT JOIN dp p ON p.nk_product_id = oi.product_id
  LEFT JOIN c_scd c
    ON c.customer_id   = o.customer_id
   AND o.order_date   >= c.eff_start_date
   AND o.order_date   <= c.eff_end_date
  WHERE o.order_date >= date_sub(current_date(), 60)  -- watermark (ajuste conforme sua latência)
)
SELECT
  order_id,
  order_item_id,
  fk_tempo_sk,
  COALESCE(fk_customer_sk, 0) AS fk_customer_sk,
  fk_product_sk,
  quantity,
  unit_price,
  discount_amount,
  CAST(quantity * unit_price AS DECIMAL(18,2))                            AS revenue_gross,
  CAST(quantity * unit_price - COALESCE(discount_amount,0) AS DECIMAL(18,2)) AS revenue_net,
  sha2(concat_ws('||',
    order_id, order_item_id,
    cast(coalesce(fk_tempo_sk,0) as string),
    cast(coalesce(fk_customer_sk,0) as string),
    cast(coalesce(fk_product_sk,0) as string),
    cast(coalesce(quantity,0) as string),
    cast(coalesce(unit_price,0) as string),
    cast(coalesce(discount_amount,0) as string),
    cast(coalesce(quantity * unit_price,0) as string)
  ),256) AS row_hash
FROM base;

-- =========================
-- MERGE idempotente
-- =========================
MERGE INTO gold.fact_vendas t
USING fact_stage s
ON  t.order_item_id = s.order_item_id
WHEN MATCHED AND (t.row_hash IS NULL OR t.row_hash <> s.row_hash) THEN UPDATE SET
  t.order_id        = s.order_id,
  t.fk_tempo_sk     = s.fk_tempo_sk,
  t.fk_customer_sk  = s.fk_customer_sk,
  t.fk_product_sk   = s.fk_product_sk,
  t.quantity        = s.quantity,
  t.unit_price      = s.unit_price,
  t.discount_amount = s.discount_amount,
  t.revenue_gross   = s.revenue_gross,
  t.revenue_net     = s.revenue_net,
  t.row_hash        = s.row_hash
WHEN NOT MATCHED THEN INSERT (
  order_id, order_item_id, fk_tempo_sk, fk_customer_sk, fk_product_sk,
  quantity, unit_price, discount_amount, revenue_gross, revenue_net, row_hash
) VALUES (
  s.order_id, s.order_item_id, s.fk_tempo_sk, s.fk_customer_sk, s.fk_product_sk,
  s.quantity, s.unit_price, s.discount_amount, s.revenue_gross, s.revenue_net, s.row_hash
);

     