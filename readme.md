# 🛡️ Sentinel SP: Inteligência de Dados e Prevenção a Fraudes

Este projeto, desenvolvido para o programa **Data Master**, implementa uma solução de Engenharia de Dados *end-to-end* em arquitetura **Data Lakehouse**. O foco é a extração e análise estratégica dos microdados de segurança pública da SSP-SP sobre roubos e furtos de dispositivos móveis, transformando-os em ativos de decisão para o setor bancário e de seguros.

---

## 🏗️ Arquitetura da Solução
O pipeline utiliza a **Metodologia de Medalhão** em nuvem AWS, garantindo integridade, escalabilidade e custo-eficiência:

* **Raw (Bronze):** Dados brutos extraídos via **AWS Lambda** e armazenados em CSV. A ingestão é orquestrada por eventos, disparando o fluxo de processamento imediatamente.
* **Trusted (Silver):** Processamento distribuído via **AWS Glue (PySpark)** para limpeza, normalização (remoção de acentos/caracteres especiais) e anonimização de dados sensíveis (PII) com **Hash SHA-256**.
* **Refined (Gold):** Dados agregados, convertidos para **Parquet** e particionados por `ANO_BO` e `MES` para alta performance de consulta no **Amazon Athena** e **QuickSight**.

## 📁 Estrutura do Repositório
```text
data-master-sentinel/
├── delivery_banca/       # PACOTE CONSOLIDADO PARA AVALIAÇÃO (Scripts e amostras).
├── docs/                 # Documentação técnica, dicionário de dados e diagramas.
├── infra/                # Templates CloudFormation (IaC) para S3, Glue e Lambda.
├── scripts/              
│   ├── lambda/           # Handler de extração e orquestração (Boto3).
│   └── glue/             # Jobs PySpark para transformações Silver e Gold.
└── README.md             # Documentação principal.
````

## 🛠️ Tecnologias Utilizadas
* Linguagem: Python 3.12 / PySpark.
* Cloud (AWS): S3, Lambda, Glue, Athena, QuickSight.
* Observabilidade: CloudWatch Alarms, Cloudtrail e SNS para notificações de falhas em tempo real.
* Infraestrutura como Código (IaC): CloudFormation.

Bibliotecas Principais: boto3, requests, pandas

## 🚀 Como Configurar e Executar
1. Pré-requisitos
* AWS CLI configurado com as credenciais necessárias.
* Python instalado (versão 3.12 recomendada).

2. Instalação
* Clone o repositório
git clone [https://github.com/seu-usuario/data-master-sentinel.git](https://github.com/seu-usuario/data-master-sentinel.git)
cd data-master-sentinel

* Crie e ative o ambiente virtual
python -m venv .venv
source .venv/bin/activate  (Linux/Mac)
        .venv\Scripts\activate  (Windows)

* Instale as dependências
pip install -r scripts/lambda/requirements.txt


## 🔧 Como Executar
* Infraestrutura: Realize o deploy dos templates na pasta /infra para provisionar o Data Lake e as permissões IAM.
* Orquestração: Configure a Lambda para disparar o Glue Workflow.
* Catálogo: Execute o Glue Crawler para disponibilizar as tabelas no Athena.

## 🚀 Proposta de Valor e Insights
* A camada Gold alimenta um dashboard geoespacial no QuickSight que permite:
* Mapa de Área: Identificação de zonas de risco por bairro para prevenção de fraudes.
* Perfil de Risco: Análise de marcas mais visadas para precificação de seguros.
* Comportamento Temporal: Distribuição de crimes por hora e dia.

## 🔐 Segurança e Governança
* LGPD: Anonimização de dados através de técnicas de hashing.
* Custo-Eficiência: Ciclo de vida S3 (Glacier) para dados frios e retenção de logs de 14 dias no CloudWatch.
* Acesso: Bucket S3 com bloqueio de acesso público e controle via IAM Roles.
* Este projeto utiliza um arquivo .gitignore rigoroso para evitar o vazamento de chaves AWS e segredos.
* O bucket S3 possui bloqueio de acesso público habilitado.
* Todo dado na camada Raw é imutável.

---
Desenvolvido por: Bruno Stinchi de Souza

