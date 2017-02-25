FROM phusion/baseimage:0.9.19

RUN apt-get update && apt-get install -y curl
RUN curl -o /usr/local/bin/wait-for-it \
    https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh && \
    chmod 755 /usr/local/bin/wait-for-it

ENTRYPOINT ["wait-for-it", "psql_server:5432"]
