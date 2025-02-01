# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

ARG LISTENER_BASE_IMAGE_TAG=0.9.0-pre-cgarcia-ucs-base-2024-12-12
ARG LISTENER_BASE_IMAGE=gitregistry.knut.univention.de/univention/customers/dataport/upx/container-listener-base/listener-base

ARG WORKDIR="/app/listener"

FROM ${LISTENER_BASE_IMAGE}:${LISTENER_BASE_IMAGE_TAG} AS builder
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ARG WORKDIR
WORKDIR ${WORKDIR}

ARG USER=udm_listener
RUN useradd -m "$USER"
RUN chown "$USER":"$USER" "$WORKDIR" \
    && apt-get --assume-yes --verbose-versions --no-install-recommends install \
      python3-pip \
    && rm -fr /var/lib/apt/lists/* /var/cache/apt/archives/*
USER "$USER"

ENV \
    PATH="/home/${USER}/.local/bin:${PATH}" \
    PYTHONPATH="." \
    WEB_CONCURRENCY=1 \
    # Do not buffer `stdout`
    PYTHONUNBUFFERED=1 \
    # Do not create `.pyc` files
    PYTHONDONTWRITEBYTECODE=1 \
    # Do not ask questions
    POETRY_NO_INTERACTION=1 \
    # Put .venv inside source folder
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    # Cache of poetry
    POETRY_CACHE_DIR=/tmp/poetry_cache

# install Poetry
RUN pip3 install --break-system-packages --no-cache-dir poetry==1.8.3

# install dependencies
COPY ./listener/poetry.lock \
     ./listener/pyproject.toml \
     ./listener/README.md \
     ${WORKDIR}/
COPY ./consumer /app/consumer
COPY ./backends /app/backends

# install dependencies
RUN poetry install --no-root --only main

# copy source code
COPY ./listener/src ${WORKDIR}/src
RUN poetry install --only main

FROM ${LISTENER_BASE_IMAGE}:${LISTENER_BASE_IMAGE_TAG} AS final
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ARG WORKDIR
WORKDIR ${WORKDIR}

# Copy patch files
COPY ./docker/udm-listener/*.patch /tmp/

# Install patch utility
# hadolint ignore=DL3008
RUN apt-get update && \
    apt-get --assume-yes --verbose-versions --no-install-recommends install \
    patch && \
    patch -p1 -i /tmp/listener.py.patch /usr/lib/python3/dist-packages/listener.py && \
    patch -p1 -i /tmp/handler_logging.py.patch /usr/lib/python3/dist-packages/univention/listener/handler_logging.py && \
    apt-get purge --auto-remove --assume-yes patch && \
    rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*

COPY ./listener/src/ldap_listener.py /usr/lib/univention-directory-listener/system/

COPY --from=builder ${WORKDIR} .
ENV PYTHONPATH=./src:./.venv/lib/python3.11/site-packages
