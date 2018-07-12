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


# install etk dependencies (install them here for cache of image building)
RUN mkdir /app/etk
RUN python3 -m venv /app/etk/etk2_env
ADD requirements.txt /app/etk

RUN cd /app/etk && source etk2_env/bin/activate && pip install -r requirements.txt

# set etk2_env as default env
ENV PATH /app/etk/etk2_env/bin:$PATH
RUN /bin/bash -c "python -m spacy download en_core_web_sm"

# add etk
ADD . /app/etk

CMD /bin/bash
