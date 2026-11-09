FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY requirements.txt ./
RUN python -m pip install --no-cache-dir -r requirements.txt \
    && groupadd --gid 10001 sentinel \
    && useradd --uid 10001 --gid sentinel --no-create-home sentinel \
    && mkdir -p /app/data \
    && chown 10001:10001 /app/data
COPY sentinel/ ./sentinel/
USER sentinel
EXPOSE 8000
CMD ["python", "-m", "sentinel.server.main"]
