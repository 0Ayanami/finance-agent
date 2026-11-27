ARG PYTHON_IMAGE=mirror.gcr.io/library/python:3.13-slim
ARG NODE_IMAGE=mirror.gcr.io/library/node:22-bookworm-slim
ARG UV_IMAGE=ghcr.io/astral-sh/uv:0.11.16
FROM ${UV_IMAGE} AS uv
FROM ${NODE_IMAGE} AS web
WORKDIR /web
ENV NEXT_TELEMETRY_DISABLED=1
COPY interfaces/web/frontend/package*.json ./
RUN npm ci --no-audit --no-fund
COPY interfaces/web/frontend/ ./
RUN npm run build

FROM ${PYTHON_IMAGE} AS dependencies
COPY --from=uv /uv /bin/uv
WORKDIR /src
ENV UV_PROJECT_ENVIRONMENT=/opt/venv UV_PYTHON_DOWNLOADS=never UV_LINK_MODE=copy
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --extra milvus --extra redis --no-install-project
COPY __init__.py __main__.py config.py ./
COPY application application
COPY core core
COPY infrastructure infrastructure
COPY interfaces interfaces
COPY services services
COPY tools tools
COPY --from=web /web/out interfaces/web/frontend/out
RUN uv sync --locked --extra milvus --extra redis --no-editable

FROM ${PYTHON_IMAGE}
ENV PATH=/opt/venv/bin:$PATH PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 HOME=/tmp
COPY --from=dependencies /opt/venv /opt/venv
COPY deployment/kubernetes/server.py /opt/app/server.py
COPY skills /opt/app/skills
RUN mkdir -p /workspace && chown 10001:10001 /workspace
WORKDIR /workspace
USER 10001:10001
EXPOSE 8000
CMD ["python", "/opt/app/server.py"]
