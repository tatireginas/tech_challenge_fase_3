
import sys
import re
import unicodedata

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME"]
)

sc = SparkContext.getOrCreate()

glueContext = GlueContext(sc)

spark = glueContext.spark_session

job = Job(glueContext)

job.init(
    args["JOB_NAME"],
    args
)


# ============================================================
# CONFIGURAÇÕES
# ============================================================

BUCKET_NAME = "data-6625-3564-2976"

DATABASE_NAME = "state_of_data"


datasets = {

    "2023": {

        "input": (
            f"s3://{BUCKET_NAME}/"
            "data-input/bronze/state-of-data/2023/"
            "State_of_data_BR_2023_Kaggle - df_survey_2023.csv"
        ),

        "output": (
            f"s3://{BUCKET_NAME}/"
            "data-output/bronze/state-of-data/2023/"
        ),

        "table": "tb_state_data_2023_bronze"
    },

    "2024": {

        "input": (
            f"s3://{BUCKET_NAME}/"
            "data-input/bronze/state-of-data/2024/"
            "Final Dataset - State of Data 2024 - Kaggle - "
            "df_survey_2024.csv"
        ),

        "output": (
            f"s3://{BUCKET_NAME}/"
            "data-output/bronze/state-of-data/2024/"
        ),

        "table": "tb_state_data_2024_bronze"
    },

    "2025_2026": {

        "input": (
            f"s3://{BUCKET_NAME}/"
            "data-input/bronze/state-of-data/2025_2026/"
            "Final Dataset - State of Data 2025-2026 - Kaggle.csv"
        ),

        "output": (
            f"s3://{BUCKET_NAME}/"
            "data-output/bronze/state-of-data/2025_2026/"
        ),

        "table": "tb_state_data_2025_2026_bronze"
    }
}

def normalizar_nome_coluna(nome):

    nome = str(nome)

    nome = unicodedata.normalize(
        "NFKD",
        nome
    )

    nome = nome.encode(
        "ascii",
        "ignore"
    ).decode(
        "ascii"
    )

    nome = nome.lower().strip()

    nome = re.sub(
        r"[^a-z0-9]+",
        "_",
        nome
    )

    nome = re.sub(
        r"_+",
        "_",
        nome
    )

    nome = nome.strip("_")

    if not nome:
        nome = "coluna"

    if nome[0].isdigit():
        nome = f"col_{nome}"

    return nome


def normalizar_colunas(df):

    nomes_originais = df.columns

    novos_nomes = []

    nomes_usados = {}

    for nome_original in nomes_originais:

        nome_base = normalizar_nome_coluna(
            nome_original
        )

        if nome_base not in nomes_usados:

            nomes_usados[nome_base] = 1
            nome_final = nome_base

        else:

            nomes_usados[nome_base] += 1

            nome_final = (
                f"{nome_base}_"
                f"{nomes_usados[nome_base]}"
            )

        novos_nomes.append(
            nome_final
        )

    return df.toDF(
        *novos_nomes
    )
def processar_bronze(
    ano,
    config
):

    print("=" * 80)
    print(f"PROCESSANDO {ano}")
    print("=" * 80)

    print(
        f"Entrada: {config['input']}"
    )

    print(
        f"Saída: {config['output']}"
    )

    dyf = glueContext.create_dynamic_frame.from_options(

        connection_type="s3",

        connection_options={
            "paths": [
                config["input"]
            ],
            "recurse": False
        },

        format="csv",

        format_options={
            "withHeader": True,
            "separator": ",",
            "quoteChar": '"'
        },

        transformation_ctx=f"read_{ano}"
    )

    df = dyf.toDF()


    print(
        f"Colunas originais: {len(df.columns)}"
    )

    df = normalizar_colunas(
        df
    )


    print(
        f"Colunas após normalização: "
        f"{len(df.columns)}"
    )


    print(
        "Exemplos de colunas:"
    )

    for coluna in df.columns[:10]:

        print(
            f" - {coluna}"
        )

    dyf_saida = DynamicFrame.fromDF(
        df,
        glueContext,
        f"dyf_saida_{ano}"
    )
    sink = glueContext.getSink(

        connection_type="s3",

        path=config["output"],

        enableUpdateCatalog=True,

        updateBehavior="UPDATE_IN_DATABASE",

        transformation_ctx=f"write_{ano}"
    )


    sink.setCatalogInfo(

        catalogDatabase=DATABASE_NAME,

        catalogTableName=config["table"]
    )


    sink.setFormat(

        "glueparquet",

        compression="snappy"
    )


    sink.writeFrame(
        dyf_saida
    )


    print(
        f"✅ {ano} concluído."
    )

    print(
        f"✅ Tabela: "
        f"{DATABASE_NAME}.{config['table']}"
    )

for ano, config in datasets.items():

    processar_bronze(
        ano,
        config
    )


# ============================================================
# FINALIZAÇÃO
# ============================================================

job.commit()

print("=" * 80)
print("✅ CAMADA BRONZE FINALIZADA")
print("=" * 80)
