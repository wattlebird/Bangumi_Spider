FROM continuumio/miniconda3
WORKDIR /code

RUN apt-get update \
    && apt-get install -y wget curl gawk jq build-essential unzip

RUN conda install -y numpy scipy pandas pyarrow \
    && pip install rankit opencc-python-reimplemented dateparser azure-storage-blob

RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash

ADD comprehensive.py split_collections.py customrank.py customtags.py generate_matrix.py record.py wikiparser.py work.sh /code/

ENTRYPOINT ["bash", "work.sh"]