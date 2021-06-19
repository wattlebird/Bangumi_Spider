FROM continuumio/miniconda3
WORKDIR /code

RUN apt-get update \
    && apt-get install -y wget curl gawk build-essential

RUN conda install -y numpy scipy pandas cython \
    && pip install rankit

RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash

ADD customrank.py customtags.py generate_matrix.py work.sh /code/

ENTRYPOINT ["bash", "work.sh"]