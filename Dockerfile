FROM python:3.7-alpine as build

RUN apk --update --no-cache --virtual=build-dependencies add \
        build-base python3-dev libxml2-dev libxslt-dev postgresql-dev && \
    apk --update --no-cache add libstdc++ libpq && \
    apk --repository http://dl-3.alpinelinux.org/alpine/edge/community/ --update --no-cache add leveldb leveldb-dev && \
    pip install --no-cache-dir dgp_server && \
    apk del build-dependencies && rm -rf /var/cache/apk/*
RUN mkdir -p /var/dgp

FROM build

RUN pip install --no-cache-dir dgp_server==0.0.49

ENV SERVER_MODULE=dgp_server.server:app

EXPOSE 8000

CMD gunicorn -w 4 -t 180 --bind 0.0.0.0:8000 ${SERVER_MODULE} --worker-class aiohttp.GunicornWebWorker

