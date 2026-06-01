ARG BASE_IMAGE=python:3.11-slim-bookworm
FROM ${BASE_IMAGE}

ARG TARGETARCH
ARG MONGODB_DATABASE_TOOLS_VERSION=100.13.0
ARG MONGODB_DATABASE_TOOLS_DISTRO=debian12
ARG MONGODB_DATABASE_TOOLS_ARM64_DISTRO=ubuntu2204

ENV TZ=Asia/Shanghai
ENV PYTHONPATH=/app

# 1. Debian Mirror (Aliyun)
RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/debian.sources && \
    apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    tzdata \
    git \
    procps \
    && rm -rf /var/lib/apt/lists/*

# MongoDB Database Tools provide mongodump/mongorestore for backups.
# Install the standalone tools package instead of adding a mongodb-org/7.0 apt
# repository, so the image can work against both MongoDB 6.x and 7.x servers.
RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64) mongodb_tools_arch="x86_64"; mongodb_tools_distro="${MONGODB_DATABASE_TOOLS_DISTRO}" ;; \
      arm64) mongodb_tools_arch="arm64"; mongodb_tools_distro="${MONGODB_DATABASE_TOOLS_ARM64_DISTRO}" ;; \
      *) echo "Unsupported MongoDB tools architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    tools_pkg="mongodb-database-tools-${mongodb_tools_distro}-${mongodb_tools_arch}-${MONGODB_DATABASE_TOOLS_VERSION}.deb"; \
    curl -fsSLo "/tmp/${tools_pkg}" "https://fastdl.mongodb.org/tools/db/${tools_pkg}"; \
    apt-get update; \
    apt-get install -y --no-install-recommends "/tmp/${tools_pkg}"; \
    rm -f "/tmp/${tools_pkg}"; \
    rm -rf /var/lib/apt/lists/*

# 2. Supercronic (Cron for Containers)
ARG SUPERCRONIC_VERSION=v0.2.29
# Use a proxy or mirror if needed, but direct github usually works or fails fast. 
# Attempting direct first.
RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64|arm64) supercronic_arch="${TARGETARCH}" ;; \
      *) echo "Unsupported supercronic architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    supercronic_bin="supercronic-linux-${supercronic_arch}"; \
    supercronic_url="https://ghproxy.net/https://github.com/aptible/supercronic/releases/download/${SUPERCRONIC_VERSION}/${supercronic_bin}"; \
    curl -fsSLO "${supercronic_url}"; \
    chmod +x "${supercronic_bin}"; \
    mv "${supercronic_bin}" /usr/local/bin/supercronic

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
