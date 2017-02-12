FROM postgres:9.5

RUN apt-get update && apt-get install -y curl
RUN curl -o /usr/local/bin/wait-for-it.sh \
    https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh
RUN chmod 755 /usr/local/bin/wait-for-it.sh

ENTRYPOINT ["wait-for-it.sh", "psql_server:5432", "--", "psql", "--single-transaction"]
