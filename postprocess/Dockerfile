FROM continuumio/miniconda3
WORKDIR /code

RUN apt-get update \
    && apt-get install -y wget curl gawk build-essential
ENV PATH /opt/conda/bin/:$PATH
RUN conda update -y conda \
    && conda install -y numpy scipy pandas cython \
    && pip install rankit

RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash

ADD customrank.py customtags.py work.sh record_pair_prob.spec.awk record_pair.awk record_processing.awk /code/

ENTRYPOINT ["bash", "work.sh"]