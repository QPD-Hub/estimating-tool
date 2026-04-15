FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY . .

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl gnupg2 unixodbc unixodbc-dev \
    && curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -r requirements.txt

ENV APP_ENV=production
ENV PORT=8000
ENV DOC_AUTOMATION_DROP_ROOT=/data/automation-drop
ENV DOC_WORK_ROOT=/data/work

RUN mkdir -p /data/automation-drop /data/work

EXPOSE 8000

CMD ["python", "app.py"]
