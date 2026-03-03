ARG KATSDPDOCKERBASE_REGISTRY=harbor.sdp.kat.ac.za/dpp

FROM $KATSDPDOCKERBASE_REGISTRY/docker-base-build AS build
LABEL maintainer="Richard Armstrong <richarms@sarao.ac.za>"

ENV DEBIAN_FRONTEND=noninteractive

USER kat
ENV PATH="$PATH_PYTHON3" VIRTUAL_ENV="$VIRTUAL_ENV_PYTHON3"

WORKDIR /tmp/install
COPY --chown=kat:kat requirements.txt /tmp/install/requirements.txt
COPY --chown=kat:kat pyproject.toml /tmp/install/pyproject.toml
COPY --chown=kat:kat src /tmp/install/src
RUN install_pinned.py -r /tmp/install/requirements.txt && \
    pip install --no-deps /tmp/install && \
    pip check

FROM $KATSDPDOCKERBASE_REGISTRY/docker-base-runtime
LABEL maintainer="Richard Armstrong <richarms@sarao.ac.za>"

ENV DEBIAN_FRONTEND=noninteractive

COPY --from=build --chown=kat:kat /home/kat/ve3 /home/kat/ve3

USER root
COPY scripts/vlbimeta.py /usr/local/bin/vlbimeta.py
RUN chmod +x /usr/local/bin/vlbimeta.py

RUN mkdir -p /opt/vlbimeta/catalogues /opt/vlbimeta/metadata /var/kat/data /runtime && \
    chown -R kat:kat /opt/vlbimeta /var/kat/data /runtime

USER kat
COPY --chown=kat:kat catalogues /opt/vlbimeta/catalogues
COPY --chown=kat:kat metadata /opt/vlbimeta/metadata
ENV VLBIMETA_APP_ROOT=/opt/vlbimeta \
    VLBIMETA_CATALOGUE_DIR=/opt/vlbimeta/catalogues \
    VLBIMETA_METADATA_DIR=/opt/vlbimeta/metadata
ENV PATH="$PATH_PYTHON3" VIRTUAL_ENV="$VIRTUAL_ENV_PYTHON3"

WORKDIR /runtime
CMD ["vlbimeta.py", "--help"]
