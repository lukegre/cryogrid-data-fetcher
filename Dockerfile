FROM mambaorg/micromamba:noble

# SYSTEM DEPENDENCIES - only using environment.yml and not requirements.txt
COPY environment.yml /tmp/

# install using micromamba
RUN micromamba install -y -n base -f /tmp/environment.yml  && \
    micromamba clean --all --yes

# BASH CONFIGURATION - really just to make the image a bit nicer to use
RUN echo "force_color_prompt=yes" >> ~/.bashrc && \
    echo "PS1='\[\e[32m\]\u@\h \[\e[34m\]\w\[\e[0m\]\$ '" >> ~/.bashrc && \
    echo alias "ls='ls --color=auto'" >> ~/.bashrc && \
    echo alias "ll='ls -lF'" >> ~/.bashrc && \
    echo alias "la='ls -lAF'" >> ~/.bashrc && \
    echo alias "l='ls -CF'" >> ~/.bashrc

USER root
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER mambauser
# copy the current repository into the container
COPY .. /home/mambauser/cryogrid-era5-downloader
WORKDIR /home/mambauser/cryogrid-era5-downloader

# default entrypoint is bash
ENTRYPOINT ["/entrypoint.sh"]
