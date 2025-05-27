FROM postgres:17.5

COPY monitoring_db.sql /docker-entrypoint-initdb.d/