FROM python:3.7-alpine

RUN apk --update --no-cache --virtual=build-dependencies add \
        build-base python3-dev \libxml2-dev libxslt-dev postgresql-dev  && \
    apk --update --no-cache add libstdc++ libpq && \
    apk --repository http://dl-3.alpinelinux.org/alpine/edge/testing/ --update add leveldb leveldb-dev    
RUN pip install dgp_server
RUN mkdir -p /var/dgp

EXPOSE 8000

CMD gunicorn -t 180 --bind 0.0.0.0:8000 dgp_server.server:app --worker-class aiohttp.GunicornWebWorker

