FROM mambaorg/micromamba

# SYSTEM DEPENDENCIES
COPY environment.yml /tmp/
# install using micromamba
RUN micromamba install -y -n base -f /tmp/environment.yml  && \
    micromamba clean --all --yes

# BASH ALIAS CONFIGURATION
RUN echo alias "ls='ls --color=auto'" >> ~/.bashrc && \
    echo alias "ll='ls -lF'" >> ~/.bashrc && \
    echo alias "la='ls -lAF'" >> ~/.bashrc && \
    echo alias "l='ls -CF'" >> ~/.bashrc

USER root
COPY entrypoint.sh /entrypoint.sh
RUN chmod +rwx /entrypoint.sh

USER mambauser
COPY .. /myhome/cryogrid/era5-downloader/

ENTRYPOINT ["/entrypoint.sh"]
