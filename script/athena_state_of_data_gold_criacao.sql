============================================================================
-- STATE OF DATA - CAMADA GOLD NO AMAZON ATHENA
-- Banco de dados: state_of_data
-- Este arquivo contém somente a criação das 7 tabelas Gold.
-- ============================================================================



-- ============================================================================
-- 1. GOLD - DIVERSIDADE DE GÊNERO
-- ============================================================================
CREATE TABLE state_of_data.tb_state_data_gold_genero_athena
WITH (
    format = 'PARQUET',
    write_compression = 'SNAPPY',
    external_location = 's3://data-6625-3564-2976/data-output/gold-athena/state-of-data/diversidade-genero/'
) AS

WITH base AS (
    SELECT
        ano_pesquisa,
        genero,
        COUNT(*) AS quantidade
    FROM state_of_data.tb_state_data_silver
    WHERE genero IS NOT NULL
    GROUP BY
        ano_pesquisa,
        genero
)

SELECT
    ano_pesquisa,
    genero,
    quantidade,
    ROUND(
        quantidade * 100.0 /
        SUM(quantidade) OVER (
            PARTITION BY ano_pesquisa
        ),
        2
    ) AS percentual
FROM base;


-- ============================================================================
-- 2. GOLD - PERFIL PROFISSIONAL
-- ============================================================================
CREATE TABLE state_of_data.tb_state_data_gold_perfil_profissional_athena
WITH (
    format = 'PARQUET',
    write_compression = 'SNAPPY',
    external_location = 's3://data-6625-3564-2976/data-output/gold-athena/state-of-data/perfil-profissional/'
) AS

WITH base AS (
    SELECT
        ano_pesquisa,
        cargo_atual,
        senioridade,
        COUNT(*) AS quantidade
    FROM state_of_data.tb_state_data_silver
    WHERE cargo_atual IS NOT NULL
      AND senioridade IS NOT NULL
    GROUP BY
        ano_pesquisa,
        cargo_atual,
        senioridade
),

com_metricas AS (
    SELECT
        ano_pesquisa,
        cargo_atual,
        senioridade,
        quantidade,
        SUM(quantidade) OVER (
            PARTITION BY ano_pesquisa
        ) AS base_respostas_validas
    FROM base
),

totais AS (
    SELECT
        ano_pesquisa,
        COUNT(*) AS total_respondentes_ano
    FROM state_of_data.tb_state_data_silver
    GROUP BY ano_pesquisa
)

SELECT
    m.ano_pesquisa,
    m.cargo_atual,
    m.senioridade,
    m.quantidade,
    ROUND(
        m.quantidade * 100.0 /
        m.base_respostas_validas,
        2
    ) AS percentual,
    m.base_respostas_validas,
    t.total_respondentes_ano,
    ROUND(
        m.base_respostas_validas * 100.0 /
        t.total_respondentes_ano,
        2
    ) AS cobertura_percentual
FROM com_metricas m
LEFT JOIN totais t
    ON m.ano_pesquisa = t.ano_pesquisa;


-- ============================================================================
-- 3. GOLD - REMUNERAÇÃO
-- ============================================================================
CREATE TABLE state_of_data.tb_state_data_gold_remuneracao_athena
WITH (
    format = 'PARQUET',
    write_compression = 'SNAPPY',
    external_location = 's3://data-6625-3564-2976/data-output/gold-athena/state-of-data/remuneracao/'
) AS

WITH base AS (
    SELECT
        ano_pesquisa,
        senioridade,
        faixa_salarial,
        ordem_faixa_salarial AS ordem_faixa,
        COUNT(*) AS quantidade
    FROM state_of_data.tb_state_data_silver
    WHERE faixa_salarial IS NOT NULL
      AND senioridade IS NOT NULL
    GROUP BY
        ano_pesquisa,
        senioridade,
        faixa_salarial,
        ordem_faixa_salarial
),

metricas AS (
    SELECT
        ano_pesquisa,
        senioridade,
        faixa_salarial,
        quantidade,
        ordem_faixa,
        ROUND(
            quantidade * 100.0 /
            SUM(quantidade) OVER (
                PARTITION BY ano_pesquisa, senioridade
            ),
            2
        ) AS percentual
    FROM base
)

SELECT
    ano_pesquisa,
    senioridade,
    faixa_salarial,
    quantidade,
    ordem_faixa,
    percentual
FROM metricas;


-- ============================================================================
-- 4. GOLD - REGIÃO
-- ============================================================================
CREATE TABLE state_of_data.tb_state_data_gold_regiao_athena
WITH (
    format = 'PARQUET',
    write_compression = 'SNAPPY',
    external_location = 's3://data-6625-3564-2976/data-output/gold-athena/state-of-data/regiao/'
) AS

WITH base AS (
    SELECT
        ano_pesquisa,
        regiao,
        COUNT(*) AS quantidade
    FROM state_of_data.tb_state_data_silver
    WHERE regiao IS NOT NULL
    GROUP BY
        ano_pesquisa,
        regiao
),

metricas AS (
    SELECT
        ano_pesquisa,
        regiao,
        quantidade,
        SUM(quantidade) OVER (
            PARTITION BY ano_pesquisa
        ) AS base_respostas_validas
    FROM base
),

totais AS (
    SELECT
        ano_pesquisa,
        COUNT(*) AS total_respondentes_ano
    FROM state_of_data.tb_state_data_silver
    GROUP BY ano_pesquisa
)

SELECT
    m.ano_pesquisa,
    m.regiao,
    m.quantidade,
    ROUND(
        m.quantidade * 100.0 /
        m.base_respostas_validas,
        2
    ) AS percentual,
    m.base_respostas_validas,
    t.total_respondentes_ano,
    ROUND(
        m.base_respostas_validas * 100.0 /
        t.total_respondentes_ano,
        2
    ) AS cobertura_percentual
FROM metricas m
LEFT JOIN totais t
    ON m.ano_pesquisa = t.ano_pesquisa;


-- ============================================================================
-- 5. GOLD - MODELO DE TRABALHO
-- ============================================================================
CREATE TABLE state_of_data.tb_state_data_gold_modelo_trabalho_athena
WITH (
    format = 'PARQUET',
    write_compression = 'SNAPPY',
    external_location = 's3://data-6625-3564-2976/data-output/gold-athena/state-of-data/modelo-trabalho/'
) AS

WITH modelos AS (

    SELECT
        ano_pesquisa,
        'Atual' AS tipo_modelo,
        modelo_trabalho_atual AS modelo_trabalho,
        COUNT(*) AS quantidade
    FROM state_of_data.tb_state_data_silver
    WHERE modelo_trabalho_atual IS NOT NULL
    GROUP BY
        ano_pesquisa,
        modelo_trabalho_atual

    UNION ALL

    SELECT
        ano_pesquisa,
        'Ideal' AS tipo_modelo,
        modelo_trabalho_ideal AS modelo_trabalho,
        COUNT(*) AS quantidade
    FROM state_of_data.tb_state_data_silver
    WHERE modelo_trabalho_ideal IS NOT NULL
    GROUP BY
        ano_pesquisa,
        modelo_trabalho_ideal
),

metricas AS (
    SELECT
        ano_pesquisa,
        tipo_modelo,
        modelo_trabalho,
        quantidade,
        SUM(quantidade) OVER (
            PARTITION BY ano_pesquisa, tipo_modelo
        ) AS base_respostas_validas
    FROM modelos
),

totais AS (
    SELECT
        ano_pesquisa,
        COUNT(*) AS total_respondentes_ano
    FROM state_of_data.tb_state_data_silver
    GROUP BY ano_pesquisa
)

SELECT
    m.ano_pesquisa,
    m.tipo_modelo,
    m.modelo_trabalho,
    m.quantidade,
    ROUND(
        m.quantidade * 100.0 /
        m.base_respostas_validas,
        2
    ) AS percentual,
    m.base_respostas_validas,
    t.total_respondentes_ano,
    ROUND(
        m.base_respostas_validas * 100.0 /
        t.total_respondentes_ano,
        2
    ) AS cobertura_percentual
FROM metricas m
LEFT JOIN totais t
    ON m.ano_pesquisa = t.ano_pesquisa;


-- ============================================================================
-- 6. GOLD - TECNOLOGIAS
-- ============================================================================
CREATE TABLE state_of_data.tb_state_data_gold_tecnologias_athena
WITH (
    format = 'PARQUET',
    write_compression = 'SNAPPY',
    external_location = 's3://data-6625-3564-2976/data-output/gold-athena/state-of-data/tecnologias/'
) AS

WITH tecnologias AS (

    SELECT
        ano_pesquisa,
        id_respondente,
        'Linguagem preferida' AS categoria,
        TRIM(linguagem_preferida) AS tecnologia
    FROM state_of_data.tb_state_data_silver
    WHERE ano_pesquisa <> '2025_2026'
      AND linguagem_preferida IS NOT NULL
      AND TRIM(linguagem_preferida) <> ''

    UNION ALL

    SELECT
        s.ano_pesquisa,
        s.id_respondente,
        'Linguagem preferida' AS categoria,
        TRIM(t.tecnologia) AS tecnologia
    FROM state_of_data.tb_state_data_silver s
    CROSS JOIN UNNEST(
        SPLIT(s.linguagem_preferida, ',')
    ) AS t(tecnologia)
    WHERE s.ano_pesquisa = '2025_2026'
      AND s.linguagem_preferida IS NOT NULL
      AND TRIM(s.linguagem_preferida) <> ''

    UNION ALL

    SELECT
        s.ano_pesquisa,
        s.id_respondente,
        'Banco de dados utilizado' AS categoria,
        TRIM(t.tecnologia) AS tecnologia
    FROM state_of_data.tb_state_data_silver s
    CROSS JOIN UNNEST(
        SPLIT(s.bancos_dados, ',')
    ) AS t(tecnologia)
    WHERE s.bancos_dados IS NOT NULL
      AND TRIM(s.bancos_dados) <> ''

    UNION ALL

    SELECT
        ano_pesquisa,
        id_respondente,
        'Cloud preferida' AS categoria,
        TRIM(cloud_preferida) AS tecnologia
    FROM state_of_data.tb_state_data_silver
    WHERE cloud_preferida IS NOT NULL
      AND TRIM(cloud_preferida) <> ''

    UNION ALL

    SELECT
        s.ano_pesquisa,
        s.id_respondente,
        'Ferramenta de BI utilizada' AS categoria,
        TRIM(t.tecnologia) AS tecnologia
    FROM state_of_data.tb_state_data_silver s
    CROSS JOIN UNNEST(
        SPLIT(s.ferramenta_bi, ',')
    ) AS t(tecnologia)
    WHERE s.ferramenta_bi IS NOT NULL
      AND TRIM(s.ferramenta_bi) <> ''

    UNION ALL

    SELECT
        ano_pesquisa,
        id_respondente,
        'Ferramenta de BI preferida' AS categoria,
        TRIM(bi_preferida) AS tecnologia
    FROM state_of_data.tb_state_data_silver
    WHERE bi_preferida IS NOT NULL
      AND TRIM(bi_preferida) <> ''
),

tecnologias_distintas AS (
    SELECT DISTINCT
        ano_pesquisa,
        id_respondente,
        categoria,
        tecnologia
    FROM tecnologias
    WHERE tecnologia IS NOT NULL
      AND TRIM(tecnologia) <> ''
),

bases AS (
    SELECT
        ano_pesquisa,
        categoria,
        COUNT(DISTINCT id_respondente) AS base_respostas_validas
    FROM tecnologias_distintas
    GROUP BY
        ano_pesquisa,
        categoria
),

quantidades AS (
    SELECT
        ano_pesquisa,
        categoria,
        tecnologia,
        COUNT(DISTINCT id_respondente) AS quantidade
    FROM tecnologias_distintas
    GROUP BY
        ano_pesquisa,
        categoria,
        tecnologia
),

totais AS (
    SELECT
        ano_pesquisa,
        COUNT(*) AS total_respondentes_ano
    FROM state_of_data.tb_state_data_silver
    GROUP BY ano_pesquisa
),

metricas AS (
    SELECT
        q.ano_pesquisa,
        q.categoria,
        q.tecnologia,
        q.quantidade,
        b.base_respostas_validas,
        t.total_respondentes_ano,

        ROUND(
            q.quantidade * 100.0 /
            b.base_respostas_validas,
            2
        ) AS percentual_respondentes,

        ROUND(
            b.base_respostas_validas * 100.0 /
            t.total_respondentes_ano,
            2
        ) AS cobertura_percentual,

        CASE
            WHEN q.categoria IN (
                'Banco de dados utilizado',
                'Ferramenta de BI utilizada'
            )
                THEN 'Múltipla'

            WHEN q.categoria = 'Linguagem preferida'
             AND q.ano_pesquisa = '2025_2026'
                THEN 'Múltipla'

            ELSE 'Única'
        END AS tipo_resposta

    FROM quantidades q

    LEFT JOIN bases b
        ON q.ano_pesquisa = b.ano_pesquisa
       AND q.categoria = b.categoria

    LEFT JOIN totais t
        ON q.ano_pesquisa = t.ano_pesquisa
)

SELECT
    ano_pesquisa,
    categoria,
    tecnologia,
    quantidade,
    base_respostas_validas,
    total_respondentes_ano,
    percentual_respondentes,
    cobertura_percentual,
    tipo_resposta,

    ROW_NUMBER() OVER (
        PARTITION BY ano_pesquisa, categoria
        ORDER BY quantidade DESC
    ) AS ranking

FROM metricas;


-- ============================================================================
-- 7. GOLD - INTELIGÊNCIA ARTIFICIAL
-- ============================================================================
CREATE TABLE state_of_data.tb_state_data_gold_ia_athena
WITH (
    format = 'PARQUET',
    write_compression = 'SNAPPY',
    external_location = 's3://data-6625-3564-2976/data-output/gold-athena/state-of-data/inteligencia-artificial/'
) AS

WITH ia_base AS (

    SELECT
        ano_pesquisa,
        id_respondente,
        'Prioridade da IA na empresa' AS indicador,
        ia_prioridade_categoria AS categoria
    FROM state_of_data.tb_state_data_silver
    WHERE ia_prioridade_categoria IS NOT NULL

    UNION ALL

    SELECT
        ano_pesquisa,
        id_respondente,
        'Adoção de IA no trabalho' AS indicador,
        adocao_ia_trabalho AS categoria
    FROM state_of_data.tb_state_data_silver
    WHERE adocao_ia_trabalho IS NOT NULL
),

bases AS (
    SELECT
        ano_pesquisa,
        indicador,
        COUNT(DISTINCT id_respondente) AS base_respostas_validas
    FROM ia_base
    GROUP BY
        ano_pesquisa,
        indicador
),

quantidades AS (
    SELECT
        ano_pesquisa,
        indicador,
        categoria,
        COUNT(DISTINCT id_respondente) AS quantidade
    FROM ia_base
    GROUP BY
        ano_pesquisa,
        indicador,
        categoria
),

totais AS (
    SELECT
        ano_pesquisa,
        COUNT(*) AS total_respondentes_ano
    FROM state_of_data.tb_state_data_silver
    GROUP BY ano_pesquisa
)

SELECT
    q.ano_pesquisa,
    q.indicador,
    q.categoria,
    q.quantidade,
    b.base_respostas_validas,
    t.total_respondentes_ano,

    ROUND(
        q.quantidade * 100.0 /
        b.base_respostas_validas,
        2
    ) AS percentual,

    ROUND(
        b.base_respostas_validas * 100.0 /
        t.total_respondentes_ano,
        2
    ) AS cobertura_percentual

FROM quantidades q

LEFT JOIN bases b
    ON q.ano_pesquisa = b.ano_pesquisa
   AND q.indicador = b.indicador

LEFT JOIN totais t
    ON q.ano_pesquisa = t.ano_pesquisa;
