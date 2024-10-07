FROM apache/airflow:latest

USER root

# Instalar Git
RUN apt-get update && \
    apt-get -y install git && \
    apt-get clean

# Copiar o arquivo requirements.txt para o contêiner
COPY requirements.txt .

# Mudar para o usuário airflow
USER airflow

# Instalar dependências do Python
RUN pip install --no-cache-dir -r requirements.txt

USER airflow