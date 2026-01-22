-- ============================================
-- GOLD: dim_tempo (INSERT-ONLY, idempotente)
-- ============================================

USE CATALOG workshop_modelagem;
CREATE SCHEMA IF NOT EXISTS gold;

-- Range de datas derivado da Silver
CREATE OR REPLACE TEMP VIEW tempo_range AS
SELECT
  COALESCE(MIN(order_date), DATE('2019-01-01')) AS dt_min,
  COALESCE(MAX(order_date), DATE('2030-12-31')) AS dt_max
FROM silver.orders_clean;

-- Série diária
CREATE OR REPLACE TEMP VIEW tempo_series AS
WITH r AS (SELECT dt_min, dt_max FROM tempo_range),
seq AS (SELECT sequence(dt_min, dt_max, INTERVAL 1 DAY) AS dts FROM r)
SELECT explode(dts) AS data FROM seq;

-- Mapeamentos estáveis (sem depender de locale)
CREATE OR REPLACE TEMP VIEW dim_tempo_stage AS
WITH base AS (
  SELECT
    CAST(date_format(data, 'yyyyMMdd') AS INT) AS tempo_sk,
    data,
    YEAR(data)  AS ano,
    QUARTER(data) AS trimestre,
    MONTH(data) AS mes,
    DAY(data)   AS dia,
    CASE WHEN dayofweek(data)=1 THEN 7 ELSE dayofweek(data)-1 END AS dia_semana  -- 1=Seg ... 7=Dom
  FROM tempo_series
)
SELECT
  tempo_sk,
  CAST(date_format(data, 'yyyyMMdd') AS INT) AS date_id,
  data,
  ano,
  trimestre,
  mes,
  dia,
  dia_semana,
  -- nomes PT-BR fixos por mapeamento
  element_at(map(
    1,'janeiro', 2,'fevereiro', 3,'março', 4,'abril', 5,'maio', 6,'junho',
    7,'julho', 8,'agosto', 9,'setembro', 10,'outubro', 11,'novembro', 12,'dezembro'
  ), mes) AS nome_mes,
  element_at(map(
    1,'segunda', 2,'terça', 3,'quarta', 4,'quinta', 5,'sexta', 6,'sábado', 7,'domingo'
  ), dia_semana) AS nome_dia
FROM base;

-- Tabela alvo
CREATE TABLE IF NOT EXISTS gold.dim_tempo (
  tempo_sk   INT,
  date_id    INT,          -- yyyymmdd
  data       DATE,
  ano        INT,
  trimestre  INT,
  mes        INT,
  dia        INT,
  dia_semana INT,          -- 1=Seg ... 7=Dom
  nome_mes   STRING,
  nome_dia   STRING
) USING DELTA;

-- MERGE INSERT-ONLY (não faz UPDATE nunca)
MERGE INTO gold.dim_tempo t
USING dim_tempo_stage s
ON t.tempo_sk = s.tempo_sk
WHEN NOT MATCHED THEN INSERT (
  tempo_sk, date_id, data, ano, trimestre, mes, dia, dia_semana, nome_mes, nome_dia
) VALUES (
  s.tempo_sk, s.date_id, s.data, s.ano, s.trimestre, s.mes, s.dia, s.dia_semana, s.nome_mes, s.nome_dia
);

     