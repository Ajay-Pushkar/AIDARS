# M10.13: smallest reproducible deployment mechanism for the AIDAR
# distributed coordinator/worker processes. One image serves both roles
# (aidars-coordinator / aidars-worker, both installed as console scripts
# by pyproject.toml) -- which process runs is selected entirely by the
# command passed at `docker run`/compose time, so there is nothing
# role-specific baked into the image itself.
FROM python:3.11-slim AS base

WORKDIR /app

# Only the files needed to install the package -- keeps the build
# context/layer cache tight and avoids shipping test fixtures, docs, or
# the repo's ad hoc root-level dev scripts into the image.
COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --no-cache-dir .

# Runs as a non-root user; writable data directory for SQLite state /
# CAS / workload sandboxes is created and owned by that user up front.
RUN useradd --create-home --uid 10001 aidars \
    && mkdir -p /app/data \
    && chown -R aidars:aidars /app
USER aidars

ENV AIDAR_LOG_LEVEL=INFO \
    PYTHONUNBUFFERED=1

# Neither port is opened by default here -- docker-compose.yml maps the
# one relevant to whichever role a given container runs as. No CMD: the
# role (coordinator vs. worker) and every setting are supplied by the
# caller (see docker-compose.yml), so this image has no implicit default
# behavior to accidentally run in production.
