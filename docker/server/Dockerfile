# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

# TODO: Replace with version pinned date-based tag
ARG UCS_BASE_IMAGE_TAG=0.14.0-build-2024-12-12
ARG UCS_BASE_IMAGE=gitregistry.knut.univention.de/univention/components/ucs-base-image/ucs-base-520

ARG WORKDIR="/app/server"
ARG USER=provisioning-server

FROM ${UCS_BASE_IMAGE}:${UCS_BASE_IMAGE_TAG} AS build
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
ARG WORKDIR
ARG USER
WORKDIR ${WORKDIR}

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
    # Enable the debian based udm-rest-api installation
    POETRY_VIRTUALENVS_OPTIONS_SYSTEM_SITE_PACKAGES=1 \
    # Cache of poetry
    POETRY_CACHE_DIR=/tmp/poetry_cache

# install Poetry
RUN pip3 install --break-system-packages --no-cache-dir poetry==1.8.3

# install dependencies
COPY ./server/poetry.lock \
     ./server/pyproject.toml \
     ./server/README.md \
     ${WORKDIR}/
COPY ./consumer /app/consumer
COPY ./backends /app/backends

# Activate system packages to get the debian installed udm library
# RUN poetry config virtualenvs.options.system-site-packages true && \
RUN poetry install --no-root --only main

# copy source code
COPY ./server/src ${WORKDIR}/src
RUN poetry install --only main

###############################################
# Stage 2: final image
FROM ${UCS_BASE_IMAGE}:${UCS_BASE_IMAGE_TAG} AS final
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
ARG WORKDIR
ARG USER
WORKDIR ${WORKDIR}

RUN useradd -m "$USER"
RUN apt-get --assume-yes --verbose-versions --no-install-recommends install \
      python3 \
      python3-univention-directory-manager-rest \
    && rm -fr /var/lib/apt/lists/* /var/cache/apt/archives/*

USER "$USER"

COPY ./docker/server/activate_venv.envsh /entrypoint.d/
COPY --from=build --chown=${USER}:${USER} ${WORKDIR} ${WORKDIR}

###############################################
# Stage 2: final image
FROM final AS udm-transformer
ARG USER

USER root
RUN apt-get --assume-yes --verbose-versions --no-install-recommends install \
      python3-univention-directory-manager-rest \
    && rm -fr /var/lib/apt/lists/* /var/cache/apt/archives/*
USER ${USER}

CMD ["udm-transformer"]
