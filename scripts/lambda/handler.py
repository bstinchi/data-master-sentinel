import requests
import boto3
from datetime import datetime
import botocore

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket_name = "data-master-sentinel-bruno-2026" 
    base_url = "https://www.ssp.sp.gov.br/assets/estatistica/transparencia/baseDados/celularesSub/CelularesSubtraidos_{}.xlsx"
    
    ano_inicial = 2017
    ano_limite = datetime.now().year + 1
    arquivos_processados = []

    for ano in range(ano_inicial, ano_limite + 1):
        file_name = f"CelularesSubtraidos_{ano}.xlsx"
        s3_path = f"raw/celulares/ano={ano}/{file_name}"
        
        try:
            s3.head_object(Bucket=bucket_name, Key=s3_path)
            continue 
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] != "404": raise e 

        url = base_url.format(ano)
        try:
            res = requests.head(url, allow_redirects=True)
            if res.status_code == 200:
                data = requests.get(url).content
                s3.put_object(Bucket=bucket_name, Key=s3_path, Body=data)
                arquivos_processados.append(file_name)
            elif ano > datetime.now().year: break
        except Exception as err: print(f"Erro {ano}: {err}")

    return {'statusCode': 200, 'body': f"Novos arquivos: {arquivos_processados}"}