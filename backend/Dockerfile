FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 DATA_DIR=/data
WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Unprivileged user; /data holds SQLite metadata and temporary Parquet staging files (EBS-backed volume)
RUN useradd --system --uid 10001 flowstore && mkdir -p /data && chown flowstore /data
USER flowstore
VOLUME ["/data"]
EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=4s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/api/health', timeout=3).status == 200 else 1)"

# One worker on purpose: the run engine and WebSocket hub are in-process singletons.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--no-access-log"]
