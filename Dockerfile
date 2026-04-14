FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY . .

ENV APP_ENV=production
ENV PORT=8000
ENV DOC_AUTOMATION_DROP_ROOT=/data/automation-drop
ENV DOC_WORK_ROOT=/data/work

RUN mkdir -p /data/automation-drop /data/work

EXPOSE 8000

CMD ["python", "app.py"]
