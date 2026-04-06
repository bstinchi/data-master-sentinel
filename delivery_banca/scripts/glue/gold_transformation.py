import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from awsglue.dynamicframe import DynamicFrame

# Inicialização
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- CONFIGURAÇÕES DE CAMINHOS ---
path_silver = "s3://data-master-sentinel-bruno-2026/trusted/celulares/"
path_referencia = "s3://data-master-sentinel-bruno-2026/datasets/bairros_sp.csv"
gold_path = "s3://data-master-sentinel-bruno-2026/gold/celulares_analytics/"

# 1. LEITURA DOS DADOS (INCREMENTAL)
dyf_silver = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [path_silver]},
    format="parquet",
    transformation_ctx="leitura_silver_bookmark"
)

df_silver = dyf_silver.toDF()

# 2. LEITURA DA REFERÊNCIA (BAIRROS OFICIAIS)
df_ref_bairros = spark.read.csv(path_referencia, header=True, sep=",")

# 3. PROCESSAMENTO
if df_silver.count() > 0:
    
    # FUNÇÃO DE NORMALIZAÇÃO (MANTÉM ESPAÇOS, REMOVE NÚMEROS E SÍMBOLOS)
    def normalize_text(col_name):
        com_acento = "áàãâäéèêëíìîïóòõôöúùûüçÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇ"
        sem_acento = "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC"
        
        # 1. Tira acentos e coloca em MAIÚSCULO
        texto_base = F.upper(F.translate(F.col(col_name), com_acento, sem_acento))
        
        # 2. Mantém apenas letras (A-Z) e espaços (\s), removendo números e símbolos
        # 3. O segundo regexp_replace remove espaços duplos
        return F.trim(
            F.regexp_replace(
                F.regexp_replace(texto_base, "[^A-Z\s]", ""), 
                " +", " "
            )
        )

    # PREPARANDO A REFERÊNCIA DE BAIRROS
    df_ref_clean = df_ref_bairros.withColumn(
        "BAIRRO_REF_CLEAN", 
        normalize_text(df_ref_bairros.columns[0])
    ).select("BAIRRO_REF_CLEAN").distinct()

    # TRANSFORMAÇÕES, MASCARAMENTO E TIPAGEM
    df_gold_raw = df_silver \
        .withColumn("BAIRRO_CLEAN", normalize_text("BAIRRO")) \
        .withColumn("CIDADE_CLEAN", normalize_text("CIDADE")) \
        .withColumn("MARCA_CELULAR_CLEAN", normalize_text("MARCA_OBJETO")) \
        .withColumn("NOME_DELEGACIA_HASH", F.sha2(F.col("NOME_DELEGACIA"), 256)) \
        .withColumn("DATA_FATO", F.to_date(F.col("DATA_OCORRENCIA_BO"), "yyyy-MM-dd")) \
        .withColumn("HORA_FATO", F.hour(F.col("HORA_OCORRENCIA"))) \
        .withColumn("latitude", F.col("LATITUDE").cast("double")) \
        .withColumn("longitude", F.col("LONGITUDE").cast("double")) \
        .filter(
            F.col("latitude").isNotNull() & 
            (~F.isnan(F.col("latitude"))) & 
            (F.col("longitude").isNotNull()) & 
            (~F.isnan(F.col("longitude")))
        )

    # 4. TRAZ APENAS O QUE ESTÁ NA REFERÊNCIA DE BAIRROS (INNER JOIN)
    df_gold_final = df_gold_raw.join(
        df_ref_clean, 
        df_gold_raw.BAIRRO_CLEAN == df_ref_clean.BAIRRO_REF_CLEAN, 
        "inner"
    ).drop("BAIRRO_REF_CLEAN")

    # 5. ESCRITA NA CAMADA GOLD
    df_gold_final.write.mode("append").partitionBy("ANO_BO").parquet(gold_path)

job.commit()