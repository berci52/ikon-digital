# Dockerfile
FROM python:3.12-slim

# install OS-level image libs before Pillow
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      libjpeg-dev \
      zlib1g-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY exporter.py .
CMD ["python", "exporter.py"]
