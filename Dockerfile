# ETK base for all
FROM ubuntu:16.04

# all packages and environments are in /app
WORKDIR /app

## install required command utils
RUN apt-get update && apt-get install -y \
    locales \
    build-essential \
    python \
    python-dev \
    git \
    wget \
    curl \
    vim
RUN locale-gen en_US.UTF-8

# install pip
RUN wget https://bootstrap.pypa.io/get-pip.py && \
    python get-pip.py

# install conda
RUN wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    chmod +x Miniconda3-latest-Linux-x86_64.sh && \
    ./Miniconda3-latest-Linux-x86_64.sh -p /app/miniconda -b && \
    rm Miniconda3-latest-Linux-x86_64.sh
ENV PATH=/app/miniconda/bin:${PATH}
RUN conda update -y conda

# install etk dependencies (install them here for cache of image building)
RUN mkdir /app/etk
ADD environment.yml /app/etk

# create and config conda-env for etk
RUN cd /app/etk && conda-env create .
# set etk2_env as default env
ENV PATH /app/miniconda/envs/etk2_env/bin:$PATH
RUN /bin/bash -c "python -m spacy download en_core_web_sm"

# add etk
ADD . /app/etk

CMD /bin/bash
