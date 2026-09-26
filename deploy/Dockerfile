# syntax=docker/dockerfile:1
#
# futrisk image for the docker-compose deployment (01 §7.2 D2): one image, one container
# per service, the container command is `futrisk svc <name>` (see deploy/compose.yml).
#
# The build context is the repository root; deploy/Dockerfile.dockerignore limits it to
# the build inputs:
#     docker build -f deploy/Dockerfile -t futrisk:0.1.0 .
# or  docker compose -f deploy/compose.yml build

ARG PYTHON_IMAGE=python:3.12-slim
ARG NODE_IMAGE=node:20-slim

# ------------------------------------------------------------------ dashboard
FROM ${NODE_IMAGE} AS web
WORKDIR /build/web
COPY web/package.json web/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY web/ ./
# vite writes the bundle to ../src/futrisk/web_static/dist (web/vite.config.ts)
RUN mkdir -p /build/src/futrisk/web_static && npm run build

# ------------------------------------------------------------------ locked dependencies
FROM ${PYTHON_IMAGE} AS requirements
RUN pip install --no-cache-dir --disable-pip-version-check "poetry>=2.1,<3" "poetry-plugin-export>=1.9,<2"
WORKDIR /src
COPY pyproject.toml poetry.lock ./
# the main group pinned by poetry.lock, with hashes (no dev tools, no PyInstaller)
RUN poetry export --only main --format requirements.txt --output /requirements.txt

# ------------------------------------------------------------------ runtime
FROM ${PYTHON_IMAGE}
ENV PYTHONUNBUFFERED=1 \
    PYTHONUTF8=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    FUTRISK_HOME=/var/lib/futrisk

# report and backtest charts have Chinese titles: WenQuanYi Zen Hei is one of the CJK fonts
# the chart code looks for (Microsoft YaHei on Windows)
RUN apt-get update \
 && apt-get install -y --no-install-recommends fonts-wqy-zenhei \
 && rm -rf /var/lib/apt/lists/*

COPY --from=requirements /requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt && rm /tmp/requirements.txt

# The repository layout is kept (/app/src, /app/config, /app/data/hist): futrisk locates its
# bundled default config and the backtest data relative to the checkout
# (futrisk.common.paths.repo_root), so the package is installed in editable mode instead of
# as a wheel in site-packages.
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src/ ./src/
COPY config/ ./config/
COPY data/hist/ ./data/hist/
COPY --from=web /build/src/futrisk/web_static/dist/ ./src/futrisk/web_static/dist/
RUN pip install --no-deps -e . \
 && python -m compileall -q src \
 && futrisk version

# log timestamps in Beijing time, like the market clock (the code itself always uses Asia/Shanghai)
ENV TZ=Asia/Shanghai

# non-root user; <home> and the mount points of the shared volumes belong to it, so fresh named
# volumes mounted there are created with that owner
RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin futrisk \
 && mkdir -p /var/lib/futrisk/config /var/lib/futrisk/reports /var/lib/futrisk/backtests \
              /var/lib/futrisk/ticks/raw /var/lib/futrisk/ticks/canonical \
 && chown -R futrisk:futrisk /var/lib/futrisk
USER futrisk
# matplotlib's font cache, built once here instead of by the first chart of every process
RUN python -c "import matplotlib; matplotlib.use('Agg'); import matplotlib.font_manager"

EXPOSE 8000
ENTRYPOINT ["futrisk"]
CMD ["--help"]
