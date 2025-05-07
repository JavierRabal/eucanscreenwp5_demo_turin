FROM ghcr.io/cienciadedatosysalud/aspire:latest
ARG pipeline_version="Non-versioned"
ENV PIPELINE_VERSION=$pipeline_version

USER root
RUN apt update && apt install -y --no-install-recommends \
  && apt install -y xdg-utils \
  && rm -rf /var/lib/apt/lists/*

# Set time Europe/Madrid

RUN micromamba -n aspire install tzdata -c conda-forge && micromamba clean --all --yes \
    && rm -rf /opt/conda/conda-meta
ENV TZ=Europe/Madrid
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN micromamba run -n aspire date

USER $MAMBA_USER


COPY --chown=$MAMBA_USER:$MAMBA_USER env.yaml /tmp/env.yaml

# Installing dependencies
RUN micromamba install -y -n aspire -f /tmp/env.yaml \
    && micromamba run -n aspire Rscript -e "remotes::install_github('FinnishCancerRegistry/popEpi')" \
    && micromamba run -n aspire R -e "install.packages('restatapi', repos = 'https://cloud.r-project.org')" \
    && micromamba clean --all --yes \
    && rm -rf /opt/conda/conda-meta /tmp/env_project.yaml

ENV RETICULATE_PYTHON=/opt/conda/envs/aspire/bin/python

COPY --chown=$MAMBA_USER:$MAMBA_USER . /home/$MAMBA_USER/projects/
COPY --chown=$MAMBA_USER:$MAMBA_USER main_logo.png /temp/main_logo.png

RUN cp /temp/main_logo.png $(find front -name main_logo**)

ENV APP_PORT=3000
ENV APP_HOST=0.0.0.0
EXPOSE 3000

WORKDIR /home/$MAMBA_USER

ENTRYPOINT ["micromamba","run","-n","aspire","/opt/entrypoint.sh"]