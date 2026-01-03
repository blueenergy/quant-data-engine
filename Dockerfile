ARG BASE_IMAGE=ubuntu:22.04
FROM ${BASE_IMAGE}

# 镜像与包源可在 build 时覆盖，适配国内/内网环境
ARG UBUNTU_SUITE=jammy
ARG UBUNTU_MIRROR=http://mirrors.volces.com/ubuntu
ARG UBUNTU_SECURITY=http://mirrors.volces.com/ubuntu-security
ARG PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
ARG PIP_TRUSTED_HOST=pypi.tuna.tsinghua.edu.cn

ENV TZ=Asia/Shanghai
ENV PYTHONPATH=/app

# 更换为国内源
RUN sed -i "s|http://archive.ubuntu.com/ubuntu/|${UBUNTU_MIRROR}|g" /etc/apt/sources.list && \
    sed -i "s|http://security.ubuntu.com/ubuntu/|${UBUNTU_SECURITY}|g" /etc/apt/sources.list


# 安装 Python 和其他依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.11 \
    python3-pip \
    python3.11-venv \
    ca-certificates \
    tzdata \
    curl \
    wget \
    git \
    procps \
    util-linux \
    && rm -rf /var/lib/apt/lists/*

# 设置 Python 为默认版本
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.11 1 && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1

# supercronic (cron in foreground with stdout logs)
ARG SUPERCRONIC_VERSION=v0.2.3
ARG SUPERCRONIC_URL_1=https://ghproxy.com/https://github.com/aptible/supercronic/releases/download/${SUPERCRONIC_VERSION}/supercronic-linux-amd64
ARG SUPERCRONIC_URL_2=https://github.com/aptible/supercronic/releases/download/${SUPERCRONIC_VERSION}/supercronic-linux-amd64
RUN set -e; \
        for url in "${SUPERCRONIC_URL_1}" "${SUPERCRONIC_URL_2}"; do \
            echo "Downloading supercronic from ${url}"; \
            if wget -O /usr/local/bin/supercronic "${url}"; then break; fi; \
        done; \
        chmod +x /usr/local/bin/supercronic

WORKDIR /app

# 先复制 requirements.txt 以利用缓存
COPY requirements.txt /app/requirements.txt

# 设置 pip 源
RUN pip config set global.index-url ${PIP_INDEX_URL} && \
    pip config set global.trusted-host ${PIP_TRUSTED_HOST}

# 配置 pip 镜像并安装依赖（不做 editable 安装，直接用源码目录 via PYTHONPATH）
RUN pip install --no-cache-dir -r requirements.txt

# 复制其余代码
COPY . /app

RUN chmod +x /app/sync.sh

CMD ["supercronic", "/app/crontab.txt"]
