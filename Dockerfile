ARG KATSDPDOCKERBASE_REGISTRY=harbor.sdp.kat.ac.za/dpp

FROM $KATSDPDOCKERBASE_REGISTRY/docker-base-runtime
LABEL maintainer="Richard Armstrong <richarms@sarao.ac.za>"

ENV DEBIAN_FRONTEND=noninteractive

USER root
RUN apt-get update && apt-get install -y \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/vlbimeta /var/kat/data /runtime && \
    chown -R kat:kat /opt/vlbimeta /var/kat/data /runtime

USER kat
ENV PATH="$PATH_PYTHON3" VIRTUAL_ENV="$VIRTUAL_ENV_PYTHON3"

WORKDIR /tmp/install
COPY --chown=kat:kat requirements.txt /tmp/install/requirements.txt
COPY --chown=kat:kat pyproject.toml /tmp/install/pyproject.toml
COPY --chown=kat:kat src /tmp/install/src
RUN install_pinned.py -r /tmp/install/requirements.txt && \
    pip install --no-deps /tmp/install && \
    pip check

COPY --chown=kat:kat catalogues /opt/vlbimeta/catalogues
ENV VLBIMETA_APP_ROOT=/opt/vlbimeta \
    VLBIMETA_CATALOGUE_DIR=/opt/vlbimeta/catalogues \

WORKDIR /runtime
CMD ["vlbimeta.py", "--help"]
