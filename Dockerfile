FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# System packages only if needed for builds / common libs
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps first for better layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy app source
COPY . .

# Default runtime envs
ENV APP_ENV=production
ENV PORT=8000
ENV DOC_AUTOMATION_DROP_ROOT=/data/automation
ENV DOC_WORK_ROOT=/data/working

# Create app-used directories inside the container.
# These can also be replaced by mounted volumes at runtime.
RUN mkdir -p /data/automation /data/working

EXPOSE 8000

CMD ["python", "app.py"]