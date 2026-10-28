FROM postgres:17-alpine

COPY init.sql /docker-entrypoint-initdb.d/

ENV POSTGRES_PASSWORD=postgres

ENV POSTGRES_DB=crm_db

EXPOSE 5432