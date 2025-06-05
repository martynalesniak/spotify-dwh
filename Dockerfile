FROM apache/airflow:2.10.5

USER airflow


COPY ./etl/requirements.txt /requirements.txt

# Instalujemy wymagane pakiety
RUN pip install --no-cache-dir -r /requirements.txt
ENV PYTHONPATH="/opt/airflow/etl"

