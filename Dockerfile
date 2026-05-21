ARG BASE_IMAGE=python:3.11-slim
FROM ${BASE_IMAGE}

ENV TZ=Asia/Shanghai
ENV PYTHONPATH=/app

# 1. Debian Mirror (Aliyun)
RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/debian.sources && \
    apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    gnupg \
    tzdata \
    git \
    procps \
    && rm -rf /var/lib/apt/lists/*

# MongoDB Database Tools provide mongodump/mongorestore for backups.
RUN set -eux; \
    . /etc/os-release; \
    curl -fsSL https://pgp.mongodb.com/server-7.0.asc | gpg --dearmor -o /usr/share/keyrings/mongodb-server-7.0.gpg; \
    echo "deb [ signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] https://repo.mongodb.org/apt/debian ${VERSION_CODENAME}/mongodb-org/7.0 main" > /etc/apt/sources.list.d/mongodb-org-7.0.list; \
    apt-get update; \
    apt-get install -y --no-install-recommends mongodb-database-tools; \
    rm -rf /var/lib/apt/lists/*

# 2. Supercronic (Cron for Containers)
ARG SUPERCRONIC_VERSION=v0.2.29
ARG SUPERCRONIC_URL=https://ghproxy.net/https://github.com/aptible/supercronic/releases/download/${SUPERCRONIC_VERSION}/supercronic-linux-amd64
# Use a proxy or mirror if needed, but direct github usually works or fails fast. 
# Attempting direct first.
RUN curl -fsSLO "${SUPERCRONIC_URL}" && \
    chmod +x supercronic-linux-amd64 && \
    mv supercronic-linux-amd64 /usr/local/bin/supercronic

WORKDIR /app

# 3. Python Dependencies (Cached)
COPY requirements.txt .
RUN pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple && \
    pip install --no-cache-dir -r requirements.txt

# 4. Copy Code
COPY . .

# 5. Install Package (so it's pre-installed)
RUN pip install .

# 6. Permissions
RUN chmod +x /app/daily_market_sync.sh /app/start.sh

# 7. Run Entrypoint (Supercronic + Ladder Loop)
CMD ["/app/start.sh"]
