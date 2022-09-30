FROM continuumio/miniconda3
WORKDIR /code

RUN apt-get update \
    && apt-get install -y wget curl gawk jq build-essential unzip

RUN conda install -y numpy scipy pandas \
    && pip install rankit opencc-python-reimplemented

RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash

ADD customrank.py customtags.py generate_matrix.py record.py wikiparser.py work.sh /code/

ENTRYPOINT ["bash", "work.sh"]
