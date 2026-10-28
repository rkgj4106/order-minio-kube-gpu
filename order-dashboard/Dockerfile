FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt \
    && useradd --uid 10001 --create-home dashboard
COPY app.py ./
COPY static ./static
USER 10001
EXPOSE 8000
# 메모리 캐시를 공유하므로 worker는 1개. GPU는 필요하지 않습니다.
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
