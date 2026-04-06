Este diretório (/delivery_banca) contém o pacote consolidado para a avaliação técnica do projeto Data Master. 

A solução implementa um pipeline end-to-end em arquitetura Data Lakehouse na AWS.

Estrutura da Entrega:

scripts/lambda/handler.py: Código Python (Lambda) responsável pela extração e orquestração via boto3.
scripts/glue/gold_transformation.py: Script PySpark (Glue) com as transformações, normalizações e limpeza de dados.
data/bairros_sp.csv: Dataset de referência para validação e integridade geográfica (Inner Join).
data/CelularesSubtraidos_2025.xlsx: Amostra dos dados brutos de 2025 utilizados para validar o processamento.
docs/Documentação_Bruno.docx: Documentação do projeto.