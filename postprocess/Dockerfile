FROM debian:stretch
WORKDIR /code

RUN apt-get update \
    && apt-get install -y wget curl gawk build-essential \
    && wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh \
    && mkdir /root/.conda \
    && bash Miniconda3-latest-Linux-x86_64.sh -b \
    && rm -f Miniconda3-latest-Linux-x86_64.sh

ENV PATH=/root/miniconda3/bin:${PATH}
RUN conda update -y conda \
    && conda install -y numpy scipy pandas cython \
    && pip install rankit

RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash

ADD customrank.py customtags.py work.sh record_pair_prob.spec.awk record_pair.awk record_processing.awk /code/

ENTRYPOINT ["bash", "work.sh"]