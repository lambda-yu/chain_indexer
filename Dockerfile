FROM node:20-slim AS frontend
WORKDIR /build
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

FROM python:3.11-slim AS backend
WORKDIR /app
RUN pip install uv
COPY pyproject.toml uv.lock ./
RUN uv sync --no-dev --frozen --extra postgres --index-url https://pypi.tuna.tsinghua.edu.cn/simple
COPY core/ core/
COPY apps/ apps/
COPY scripts/ scripts/
COPY migrations/ migrations/
COPY alembic.ini ./
COPY --from=frontend /build/dist web/dist/

EXPOSE 8000
