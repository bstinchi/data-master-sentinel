import requests
import boto3
import pandas as pd
import io
from datetime import datetime
import botocore

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket_name = "data-master-sentinel-bruno-2026"
    base_url = "https://www.ssp.sp.gov.br/assets/estatistica/transparencia/baseDados/celularesSub/CelularesSubtraidos_{}.xlsx"
    
    ano_inicial = 2023
    ano_atual = datetime.now().year
    
    # Processar até o ano atual + 2 para pegar viradas
    for ano in range(ano_inicial, ano_atual + 2):
        file_name_xlsx = f"CelularesSubtraidos_{ano}.xlsx"
        raw_path = f"raw/celulares/ano={ano}/{file_name_xlsx}"
        trusted_path = f"trusted/celulares/ano={ano}/data.parquet"
        
        # --- ETAPA 1: Baixar arquivo raw caso seja novo ---
        try:
            s3.head_object(Bucket=bucket_name, Key=raw_path)
            print(f"[{ano}] Arquivo RAW já existe.")
        except botocore.exceptions.ClientError:
            url = base_url.format(ano)
            try:
                print(f"[{ano}] Baixando da fonte para o RAW...")
                res = requests.get(url, timeout=30)
                if res.status_code == 200:
                    s3.put_object(Bucket=bucket_name, Key=raw_path, Body=res.content)
                    print(f"[{ano}] Salvo no RAW com sucesso.")
                else:
                    if ano > ano_atual: break
                    continue
            except Exception as e:
                print(f"Erro ao baixar ano {ano}: {e}")
                continue

        # --- ETAPA 2: Converter o raw para parquet (trusted) ---
        try:
            s3.head_object(Bucket=bucket_name, Key=trusted_path)
            print(f"[{ano}] Arquivo TRUSTED já existe.")
        except botocore.exceptions.ClientError:
            print(f"[{ano}] Convertendo RAW para Parquet (Trusted)...")
            try:
                # 1. Busca o objeto e lê o conteúdo para memória
                obj = s3.get_object(Bucket=bucket_name, Key=raw_path)
                conteudo_arquivo = obj['Body'].read() 
                
                # 2. Carrega o Excel para identificar as abas
                excel_file = pd.ExcelFile(io.BytesIO(conteudo_arquivo), engine='openpyxl')
                
                # 3. Identifica a aba correta
                aba_alvo = excel_file.sheet_names[0]
                for aba in excel_file.sheet_names:
                    if 'CELULAR' in aba.upper():
                        aba_alvo = aba
                        break
                
                # 4. Lê os dados da aba selecionada
                df = pd.read_excel(excel_file, sheet_name=aba_alvo)
                
                # 5. Tratamento de Colunas e Remoção do 'ANO' interno
                df.columns = [str(c).strip().upper() for c in df.columns]
                if 'ANO' in df.columns:
                    df = df.drop(columns=['ANO'])
                
                df = df.loc[:, ~df.columns.duplicated()]
                df = df.astype(str)
                
                # 6. Conversão para Parquet e Upload
                parquet_buffer = io.BytesIO()
                df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
                s3.put_object(Bucket=bucket_name, Key=trusted_path, Body=parquet_buffer.getvalue())
                
                print(f"[{ano}] Sucesso usando a aba: {aba_alvo}")
                del df
                
            except Exception as e:
                print(f"Erro na conversão do ano {ano}: {e}")

    return {
        'statusCode': 200,
        'body': 'Processo de ingestão e conversão finalizado com sucesso!'
    }