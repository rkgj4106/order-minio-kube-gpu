FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install \
    --no-cache-dir \
    -r requirements.txt

COPY order_simulator.py .

CMD [ "python", "order_simulator.py"]