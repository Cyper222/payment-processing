FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

RUN groupadd --system service && useradd --system --gid service --no-create-home service

COPY pyproject.toml README.md requirements.lock ./
COPY src ./src
RUN pip install --no-cache-dir -c requirements.lock .

COPY alembic.ini ./
COPY alembic ./alembic
COPY scripts ./scripts

USER service
CMD ["uvicorn", "payment_service.main:app", "--host", "0.0.0.0", "--port", "8000"]

FROM base AS test
USER root
RUN pip install --no-cache-dir -c requirements.lock '.[dev]'
COPY tests ./tests
USER service
CMD ["pytest", "-q", "-p", "no:cacheprovider"]

FROM base AS runtime
