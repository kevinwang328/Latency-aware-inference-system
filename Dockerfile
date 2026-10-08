FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    "grpcio>=1.80.0" "protobuf>=6.31.1"

COPY system/ ./system/

CMD ["python", "-m", "system.api_server"]