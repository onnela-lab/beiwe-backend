FROM nginx

RUN mkdir -p /www

WORKDIR /www

RUN mkdir static

RUN rm /etc/nginx/conf.d/default.conf
COPY docker_config/nginx/dev.nginx.conf /etc/nginx/conf.d