import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
# DynamicFrame para leitura incremental com Bookmark
from awsglue.dynamicframe import DynamicFrame

# Inicialização
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 1. LEITURA INCREMENTAL 
dyf_silver = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://data-master-sentinel-bruno-2026/trusted/celulares/"]},
    format="parquet",
    transformation_ctx="leitura_silver_bookmark"
)

# Convertendo de volta para DataFrame do Spark para fazer transformações
df_silver = dyf_silver.toDF()

# Verifica se há dados novos antes de tentar processar (evita erros se rodar vazio)
if df_silver.count() > 0:
    
    # 2. FUNÇÃO DE NORMALIZAÇÃO
    def normalize_text(col_name):
        return F.upper(F.trim(F.regexp_replace(F.normalize(F.col(col_name), "NFKD"), r"[^\x00-\x7F]", "")))

    # 3. TRANSFORMAÇÕES E MASCARAMENTO PII
    df_gold = df_silver \
        .withColumn("BAIRRO_CLEAN", normalize_text("BAIRRO")) \
        .withColumn("CIDADE_CLEAN", normalize_text("CIDADE")) \
        .withColumn("MARCA_CELULAR_CLEAN", normalize_text("MARCA_CELULAR")) \
        .withColumn("NOME_ENVOLVIDO_HASH", F.sha2(F.col("NOME_ENVOLVIDO"), 256)) \
        .withColumn("DATA_FATO", F.to_date(F.col("DATA_OCORRENCIA_BO"), "yyyy-MM-dd")) \
        .withColumn("HORA_FATO", F.hour(F.col("HORA_OCORRENCIA"))) \
        .drop("NOME_ENVOLVIDO", "RG_ENVOLVIDO", "LOGRADOURO")

    # 4. ESCRITA na camada gold com o o Bookmark para garantir que só estamos lendo arquivos novos
    gold_path = "s3://data-master-sentinel-bruno-2026/gold/celulares_analytics/"
    df_gold.write.mode("append").partitionBy("ano").parquet(gold_path)

job.commit()