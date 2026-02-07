# 🛡️ Data Master Sentinel: Pipeline SSP-SP

Este projeto faz parte do programa **Data Master** e foca na construção de um pipeline de dados robusto para a extração, armazenamento e análise dos microdados de segurança pública da Secretaria de Segurança Pública de São Paulo (SSP-SP), referentes aos anos de 2025 e 2026.

## 🏗️ Arquitetura do Projeto
O pipeline utiliza uma **Medallion Architecture** (Arquitetura de Medalhão) para garantir a integridade e rastreabilidade dos dados:

* **Raw (Bronze):** Dados crus, extraídos diretamente do portal SSP via AWS Lambda, armazenados em formato CSV com particionamento por data de ingestão.
* **Trusted (Silver):** Dados limpos, tipados e convertidos para o formato colunar Parquet.
* **Refined (Gold):** Tabelas agregadas e otimizadas para consumo de BI e análise analítica.

## 📁 Estrutura do Repositório
```text
data-master-sentinel/
├── docs/                 # Dicionário de dados e diagramas de arquitetura.
├── infra/                # Templates CloudFormation para provisionamento AWS.
├── scripts/              
│   └── lambda/           # Código Python da função de extração (Scraper/Download).
├── .gitignore            # Filtro de segurança para arquivos sensíveis e caches.
└── README.md             # Documentação principal do projeto.
🛠️ Tecnologias Utilizadas
Linguagem: Python 3.12

Cloud: AWS (S3, Lambda, CloudFormation)

IaC: CloudFormation

Bibliotecas Principais: boto3, requests, pandas

🚀 Como Configurar e Executar
1. Pré-requisitos
AWS CLI configurado com as credenciais necessárias.

Python instalado (versão 3.12 recomendada).

2. Instalação
Bash
# Clone o repositório
git clone [https://github.com/seu-usuario/data-master-sentinel.git](https://github.com/seu-usuario/data-master-sentinel.git)
cd data-master-sentinel

# Crie e ative o ambiente virtual
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate   # Windows

# Instale as dependências
pip install -r scripts/lambda/requirements.txt
3. Deploy da Infraestrutura
Bash
aws cloudformation deploy \
  --template-file infra/s3-bucket.yaml \
  --stack-name sentinel-infra-s3
🛡️ Segurança e Governança
Este projeto utiliza um arquivo .gitignore rigoroso para evitar o vazamento de chaves AWS e segredos.

O bucket S3 possui bloqueio de acesso público habilitado.

Todo dado na camada Raw é imutável.

Desenvolvido por: Bruno

Contexto: Projeto Prático - Programa Data Master 2026