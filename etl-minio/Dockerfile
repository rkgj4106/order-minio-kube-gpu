FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

# 앞 단계 ETL에 필요한 MySQL/S3/Parquet 패키지.
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY etl_orders_minio.py /app/etl_orders_minio.py

RUN useradd --uid 10001 --create-home etluser
USER 10001

CMD ["python", "-u", "/app/etl_orders_minio.py"]
