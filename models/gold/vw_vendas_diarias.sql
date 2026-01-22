USE CATALOG workshop_modelagem;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE VIEW gold.vw_vendas_diarias AS
SELECT
  dt.data,
  SUM(f.revenue_net) AS receita_liquida,
  SUM(f.quantity)    AS itens
FROM gold.fact_vendas f
JOIN gold.dim_tempo dt ON dt.tempo_sk = f.fk_tempo_sk
GROUP BY dt.data;