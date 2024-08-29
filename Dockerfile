FROM mambaorg/micromamba

# SYSTEM DEPENDENCIES
COPY environment.yml /tmp/
# install using micromamba
RUN micromamba install -y -n base -f /tmp/environment.yml  && \
    micromamba clean --all --yes

# BASH CONFIGURATION
RUN echo "force_color_prompt=yes" >> ~/.bashrc && \
    echo "PS1='\[\e[32m\]\u@\h \[\e[34m\]\w\[\e[0m\]\$ '" >> ~/.bashrc && \
    echo alias "ls='ls --color=auto'" >> ~/.bashrc && \
    echo alias "ll='ls -lF'" >> ~/.bashrc && \
    echo alias "la='ls -lAF'" >> ~/.bashrc && \
    echo alias "l='ls -CF'" >> ~/.bashrc

USER root
COPY entrypoint.sh /entrypoint.sh
RUN chmod +rwx /entrypoint.sh

USER mambauser
WORKDIR /myhome/cryogrid/
SHELL ["micromamba", "run", "-n", "base", "/bin/bash", "-c"]
RUN git clone https://github.com/lukegre/cryogrid-era5-downloader.git 

ENTRYPOINT ["/entrypoint.sh"]
