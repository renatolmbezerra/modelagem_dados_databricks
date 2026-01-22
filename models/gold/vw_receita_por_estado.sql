USE CATALOG workshop_modelagem;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE VIEW gold.vw_receita_por_estado AS
SELECT
  dt.data,
  dc.state,
  SUM(f.revenue_net) AS receita_liquida
FROM gold.fact_vendas f
JOIN gold.dim_tempo   dt ON dt.tempo_sk    = f.fk_tempo_sk
JOIN gold.dim_cliente dc ON dc.customer_sk = f.fk_customer_sk
GROUP BY dt.data, dc.state;
