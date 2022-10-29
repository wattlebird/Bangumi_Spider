FROM python:3.10
WORKDIR /code

RUN apt-get update && apt-get install --no-install-recommends -y \
    nginx apache2-utils

Add requirements.txt /code
RUN pip3 install -r requirements.txt

ADD scrapyd.conf /etc/scrapyd/scrapyd.conf
ADD nginx.conf /etc/nginx/sites-enabled/default
ADD run.sh /code

EXPOSE 6810
ENTRYPOINT ["bash", "-v", "run.sh"]
