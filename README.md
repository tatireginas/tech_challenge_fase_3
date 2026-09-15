# Tech Challenge Fase 3 — Big Data & Analytics
## State of Data Brasil 2023 + 2024 + 2025/2026 — Pipeline AWS real (S3 → Glue → Bronze/Silver → Athena)

Pipeline **realmente executado** no AWS Academy Lab. Esta é a **segunda revisão** da arquitetura:
a camada Gold deixou de ser gerada por um notebook PySpark dedicado e passou a ser materializada
**direto pelo Amazon Athena** (via CTAS), eliminando um componente do pipeline sem perder nenhuma
regra de negócio.

## Histórico de versões deste projeto

| Versão | O que mudou |
|---|---|
| v1 | Pipeline real (bucket/Role reais), 2023+2024+2025/26, sem Crawler (auto-catalogação), Gold via notebook PySpark + saveAsTable |
| **v2 (atual)** | **Gold sem notebook Spark dedicado — Amazon Athena materializa as 7 tabelas direto da Silver via CTAS** |

## Estrutura do repositório

```
tech_challenge_fase_3/
├── 01_data_input/kaggle/
|       └── Final Dataset - State of Data 2024 - Kaggle - df_survey_2024  Arquivo csv copia do kaggle.
|       └── Final Dataset - State of Data 2025-2026 - Kaggle              Arquivo csv copia do kaggle.
|       └── State_of_data_BR_2023_Kaggle - df_survey_2023                 Arquivo csv copia do kaggle. 
|
├──02_notebooks/
│     └── glue-state-of-data-bronze.py   Script do Glue Job (PySpark). CSV → Parquet+Snappy,
│                                       cataloga via enableUpdateCatalog=True (sem Crawler).
|                                       Glue Notebook (PySpark). Lê as 3 tabelas Bronze via
│                                       Catalog, audita qualidade (nulos, duplicatas, separador
│                                       diferente em 2023), harmoniza ~35 campos, unifica os
│                                       3 anos, grava tb_state_data_silver_v2 (saveAsTable).
│                                       ESTA CAMADA NÃO MUDOU na v2.
|
├── 03_script/
│   ├── athena_gold_ctas_from_silver.sql       ★ ARQUIVO VIGENTE — materializa as 7 tabelas
│   │                                   Gold direto da Silver via CREATE TABLE AS SELECT,
│   │                                   traduzindo 1:1 a lógica PySpark (Window functions,
│   │                                   unpivot de tecnologias, normalização de texto livre,
│   │                                   regras de IA) para SQL Presto/Trino. Sintaxe validada
│   │                                   com sqlglot (dialeto presto).
│   └── athena_create_external_tables_gold.sql   Versão anterior — assumia que os 7 CSVs já
│                                       existiam (gerados pelo notebook Gold antigo) e criava
│                                       tabela externa sobre eles. Superada pelo CTAS acima,
│                                       mantida como alternativa caso prefiram consultar CSVs
│                                       já exportados em vez de recalcular da Silver.
├── 04_athena_outputs/                   Os 7 CSVs reais gerados pelo pipeline v1 (Gold via
│   ├── 01_diversidade_genero.csv      Spark). Servem de gabarito para validar que o CTAS do
│   ├── 02_perfil_profissional.csv     Athena (v2) produz os mesmos números.
│   ├── 03_remuneracao.csv
│   ├── 04_regiao.csv
│   ├── 05_modelo_trabalho.csv
│   ├── 06_tecnologias.csv
│   └── 07_inteligencia_artificial.csv
├── 05_arquitetura/
│   ├── Arquitetura_AWS_gold_via_athena.drawio   Diagrama vigente (v2), editável.
│   └── arquitetura_pipeline_real.png            Mesma arquitetura, renderizada.
├── 06_material_executivo/
│   ├── material_executivo_v3_dataviz_real.pptx   ★ VERSÃO VIGENTE — MATERIAL EXECUTIVO COM
│   │                                   DATAVIZ + STORYTELLING. 14 slides: contexto,
│   │                                   metodologia, arquitetura, 7 perguntas de negócio (cada
│   │                                   uma com os gráficos REAIS do time de BI — recortados de
│   │                                   Perfil_de_Mercado_fonte_BI.pptx — + leitura dos dados +
│   │                                   recomendação), riscos de qualidade, recomendações
│   │                                   estratégicas. DataViz do BI + narrativa/insights/
│   │                                   recomendações escritos por cima, como pedido no enunciado.
│   |
│   ├── Perfil_de_Mercado_fonte_BI.pptx   Fonte dos gráficos usados acima — painel de BI (Looker
│   │                                   Studio ou similar) com 4 páginas: Perfil de Mercado,
│   │                                   Adoção de IA x Prioridade, Tecnologias, Analytics &
│   │                                   Desafios. Cada página é uma imagem única; os gráficos
│   │                                   individuais foram recortados dela (ver pasta abaixo).
│   ├── graficos_bi_recortados/        Os 13 recortes individuais extraídos do BI, um por
│   │                                   gráfico/tabela, usados no PPTX vigente.
│   ├── material_executivo_real.pptx   Versão anterior (gráficos gerados via matplotlib a
│   │                                   partir dos CSVs, não os visuais do time de BI) —
│   │                                   mantida como referência, substituída pela v3.
│   ├── material_executivo_real.pdf    Idem, em PDF.
│   └── Perfil_de_Mercado_Profissionais_de_Dados.pdf   Export anterior do BI em PDF puro (sem
│                                       narrativa/recomendações) — mantido como referência.


## Arquitetura vigente (v2) — Gold via Athena

```
STATE OF DATA (CSV local: 2023 + 2024 + 2025/2026)
        ↓
VS Code / Jupyter — Python + boto3      (00_bronze/base_bronze.ipynb)
        ↓
Amazon S3 — data-input/bronze/state-of-data/{ano}/
        ↓
AWS Glue Job (PySpark) — glue-state-of-data-bronze.py
        ↓  (CSV → Parquet + Snappy, sem limpeza)
Amazon S3 — Camada Bronze — data-output/bronze/state-of-data/{ano}/
        ↓  (registra tabela — SEM Crawler, enableUpdateCatalog=True)
Glue Data Catalog — 3 tabelas Bronze
        ↓  (lê via Catalog: create_dynamic_frame.from_catalog)
AWS Glue Notebook (PySpark) — camada-silver-final-ver.ipynb
        ↓  (auditoria de qualidade + harmonização dos 3 anos)
Camada Silver — tb_state_data_silver_v2 (Parquet+Snappy, part. ano_pesquisa, saveAsTable)
        │
        │  ✂── notebook Gold + saveAsTable REMOVIDOS do fluxo ──✂
        │
        ↓  direto — 7 tabelas Gold materializadas via CTAS
Amazon Athena — athena_gold_ctas_from_silver.sql
        ↓
Exportação CSV via Athena (UNLOAD ou download do resultado da query)
        ↓
Amazon S3 — data-output/export-csv-athena/
        ↓
Download dos 7 CSVs → Gráficos/Análises → Material Executivo (PPTX/PDF)
```

## Sobre a tradução PySpark → SQL (a parte que exigia mais cuidado)

O notebook Gold antigo (`02_gold_DEPRECADO_ver_03_athena/`) não fazia só `GROUP BY` simples —
tinha lógica de negócio real: Window functions para % e ranking, um "despivot" de 6 campos de
tecnologia em formato longo (com `F.explode`/`F.split` só em alguns campos, não em todos), e
regras de normalização de texto livre (grafias equivalentes de tecnologia, categorização de
respostas de IA por prefixo de texto). Tudo isso foi traduzido **linha a linha** para SQL Athena/
Presto em `03_athena/athena_gold_ctas_from_silver.sql`:

- `Window.partitionBy(...)` → `... OVER (PARTITION BY ...)`
- `F.explode(F.split(col, ","))` → `CROSS JOIN UNNEST(split(col, ','))`
- `F.create_map(...)` (ordem das faixas salariais) → `CASE faixa_salarial WHEN ... THEN ... END`
- `F.when(...).otherwise(...)` → `CASE WHEN ... ELSE ... END`
- `.dropDuplicates([...])` → `SELECT DISTINCT`
- `F.countDistinct(...)` → `COUNT(DISTINCT ...)`

Cada uma das 7 CTAS no `.sql` tem comentário apontando pra célula correspondente do notebook
original, para facilitar auditoria cruzada. Sintaxe validada com `sqlglot` (dialeto Presto) — todas
as 15 statements (7 `DROP` + 7 `CREATE TABLE AS` + validação) parseiam sem erro.

**Atenção operacional:** os caminhos S3 das novas tabelas Gold usam um prefixo diferente
(`data-output/gold-athena/...`) do antigo (`data-output/gold/...`), de propósito — evita misturar
Parquet gravado pelo Spark com Parquet gravado pelo Athena no mesmo prefixo, o que causaria
contagem duplicada.

## Como reproduzir em um novo AWS Academy Lab

1. Trocar `BUCKET_NAME`/`ROLE_ARN` nos notebooks pelos valores da sua sessão atual do Lab.
2. Rodar `00_bronze/base_bronze.ipynb` de ponta a ponta.
3. Anexar `01_silver/camada-silver-final-ver.ipynb` a uma Glue Interactive Session e rodar.
4. Abrir o editor de queries do **Amazon Athena** e rodar
   `03_athena/athena_gold_ctas_from_silver.sql` (trocando `<DB>`/`<BUCKET>`) — isso substitui o
   notebook Gold antigo por completo.
5. Exportar os resultados (UNLOAD ou download da query) e comparar com `04_dados_finais/` para
   validar que os números batem com o pipeline v1.
6. Gerar os gráficos com `07_graficos/gerar_graficos_reais.py` e montar o material executivo
   (script de referência: peça para o Claude reconstruir o `.pptx` a partir dos novos CSVs, ou
   adapte `06_material_executivo/` manualmente).

## Rastreabilidade: pergunta de negócio → tabela Gold → slide do material executivo

| Pergunta de negócio | Tabela Gold (Athena CTAS) | Slide |
|---|---|---|
| Estrutura do mercado / perfis mais valorizados | `tb_state_data_gold_perfil_profissional` | 5 |
| Remuneração por senioridade | `tb_state_data_gold_remuneracao` | 6 |
| Diversidade de gênero | `tb_state_data_gold_genero` | 7 |
| Tecnologias mais usadas | `tb_state_data_gold_tecnologias` | 8 |
| Adoção de IA no trabalho | `tb_state_data_gold_ia` (indicador = "Adoção...") | 9 |
| Prioridade da IA na empresa | `tb_state_data_gold_ia` (indicador = "Prioridade...") | 10 |
| Modelo de trabalho | `tb_state_data_gold_modelo_trabalho` | 11 |
| Distribuição regional | `tb_state_data_gold_regiao` | 12 |

## O que NÃO existe nos dados reais (differença da versão sintética anterior)

A versão sintética anterior (arquivada) tinha uma 8ª tabela/gráfico de "satisfação e intenção de
troca de emprego" (retenção). **Essa dimensão não existe nas 7 tabelas Gold reais** — não foi
computada no notebook Gold original nem faria sentido inventá-la agora. O material executivo
atual cobre apenas o que os dados realmente sustentam: estrutura de mercado, remuneração,
diversidade, tecnologias, IA (2 ângulos), modelo de trabalho e região.

## Sobre os gráficos do time de BI usados no material executivo

`Perfil_de_Mercado_fonte_BI.pptx` tem 4 páginas exportadas como imagem única cada (não são
objetos nativos/editáveis do PowerPoint — foram gerados por uma ferramenta de BI, provavelmente
Looker Studio, e colados como PNG). Para reaproveitar cada gráfico individualmente no material
de storytelling, cada página foi recortada em suas partes (13 recortes ao todo, em
`graficos_bi_recortados/`).

Dois achados no processo, ambos já refletidos no material (slide 10 e slide 12):
- O gráfico "Modelo x Região" mostra o modelo **Ideal** (preferido), não o modelo **Atual** —
  só fica claro lendo a legenda pequena da própria imagem. Sinalizado explicitamente no slide
  para não ser lido errado.
- Duas mini-linhas ("sparklines") no topo da página "Analytics & Desafios" mostravam os mesmos
  6 valores duplicados (907 → 1.504 → 8.474 → 195 → 2.531 → 391) em dois gráficos idênticos e
  sem rótulo de unidade — aparenta ser artefato de exportação do BI, não dado interpretável.
  Foram excluídas do material executivo em vez de apresentadas sem explicação.

## Riscos de qualidade de dados (documentados também no material executivo, slide 12)

- **Mudança de metodologia em "linguagem preferida"**: 2025/26 aceita múltiplas respostas onde
  2023/2024 aceitavam só uma — o salto de SQL (~2% → 84%) reflete a mudança de pergunta.
- **Baixa cobertura em "prioridade da IA na empresa"**: só 17-20% da base respondeu por ciclo.
- **Volume de respondentes em queda**: 5.293 → 5.215 → 3.494.
- **Faixas salariais nominais**, sem correção de inflação entre 2023 e 2025/26.
- **Sem painel longitudinal** — comparações são sempre transversais por coorte.
