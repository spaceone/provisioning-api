# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

ARG UCS_BASE_IMAGE_TAG=0.14.0-build-2024-12-12
ARG UCS_BASE_IMAGE=gitregistry.knut.univention.de/univention/components/ucs-base-image/ucs-base-520

##############################################################
# Optimized for fast rebuilds (when only tests/* is changed) #
##############################################################

FROM ${UCS_BASE_IMAGE}:${UCS_BASE_IMAGE_TAG} AS test
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ARG WORKDIR="/app"

RUN apt-get --assume-yes --verbose-versions --no-install-recommends install \
      python3-pip \
    && rm -fr /var/lib/apt/lists/* /var/cache/apt/archives/*

ENV \
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
    # Enable the debian based udm-rest-api installation
    POETRY_VIRTUALENVS_OPTIONS_SYSTEM_SITE_PACKAGES=1 \
    # Cache of poetry
    POETRY_CACHE_DIR=/tmp/poetry_cache

# install Poetry
RUN pip3 install --break-system-packages --no-cache-dir poetry==1.8.3

# Activate the virtualenv created by poetry
# install dependencies
COPY ./poetry.lock \
     ./pyproject.toml \
     ./README.md \
     ${WORKDIR}/
COPY ./backends ${WORKDIR}/backends
COPY ./consumer ${WORKDIR}/consumer
COPY ./consumer_example ${WORKDIR}/consumer_example
COPY ./listener ${WORKDIR}/listener
COPY ./server ${WORKDIR}/server

WORKDIR ${WORKDIR}
RUN poetry install
COPY ./docker/test/activate_venv.envsh /entrypoint.d/
