FROM nginx:alpine-slim

RUN mkdir -p /www/static && \
    rm /etc/nginx/conf.d/default.conf

WORKDIR /www

COPY docker_management/nginx/dev.nginx.conf /etc/nginx/conf.d