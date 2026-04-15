FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY . .

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
        gnupg2 \
        unixodbc \
        unixodbc-dev \
    && curl -sSL -O https://packages.microsoft.com/config/debian/12/packages-microsoft-prod.deb \
    && dpkg -i packages-microsoft-prod.deb \
    && rm packages-microsoft-prod.deb \
    && mkdir -p /opt/microsoft/msodbcsql18 \
    && touch /opt/microsoft/msodbcsql18/ACCEPT_EULA \
    && apt-get update \
    && apt-get install -y --no-install-recommends msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -r requirements.txt

ENV APP_ENV=production
ENV PORT=8000
ENV DOC_AUTOMATION_DROP_ROOT=/data/automation-drop
ENV DOC_WORK_ROOT=/data/work

RUN mkdir -p /data/automation-drop /data/work

EXPOSE 8000

CMD ["python", "app.py"]