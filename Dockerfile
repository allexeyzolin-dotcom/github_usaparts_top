FROM python:3.11-slim
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends chromium postgresql-client rclone ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app /app/app
COPY Dockerfile docker-compose.vps.yml stack.yml stack.env.example PORTAINER_DEPLOY.md /app/deploy/
EXPOSE 8080
CMD ["python", "-m", "app.app"]
