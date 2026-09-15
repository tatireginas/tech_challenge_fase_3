-- ============================================================================
-- STATE OF DATA - SELECTS FINAIS PARA EXPORTAÇÃO DOS CSVs
-- Banco de dados: state_of_data
========================


-- 01_diversidade_genero.csv
SELECT *
FROM state_of_data.tb_state_data_gold_genero_athena
ORDER BY ano_pesquisa, quantidade DESC;


-- 02_perfil_profissional.csv
SELECT *
FROM state_of_data.tb_state_data_gold_perfil_profissional_athena
ORDER BY ano_pesquisa, cargo_atual, senioridade;


-- 03_remuneracao.csv
SELECT *
FROM state_of_data.tb_state_data_gold_remuneracao_athena
ORDER BY ano_pesquisa, senioridade, ordem_faixa;


-- 04_regiao.csv
SELECT *
FROM state_of_data.tb_state_data_gold_regiao_athena
ORDER BY ano_pesquisa, quantidade DESC;


-- 05_modelo_trabalho.csv
SELECT *
FROM state_of_data.tb_state_data_gold_modelo_trabalho_athena
ORDER BY ano_pesquisa, tipo_modelo, quantidade DESC;


-- 06_tecnologias.csv
SELECT *
FROM state_of_data.tb_state_data_gold_tecnologias_athena
ORDER BY ano_pesquisa, categoria, ranking;


-- 07_inteligencia_artificial.csv
SELECT *
FROM state_of_data.tb_state_data_gold_ia_athena
ORDER BY ano_pesquisa, indicador, quantidade DESC;


-- 08_base_consolidada.csv
SELECT *
FROM state_of_data.tb_state_data_silver
ORDER BY ano_pesquisa, id_respondente;
