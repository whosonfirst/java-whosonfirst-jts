#!/usr/bin/env bash

export ELS_MAIN2_HOST=localhost
export ELS_MAIN2_PORT=9200

export PSQL_MAIN_HOST=localhost
export PSQL_MAIN_DATABASE=postgres
export PSQL_MAIN_PORT=5432
export PSQL_MAIN_USER=postgres
export PSQL_MAIN_PASSWORD=secret

python src/main/python/import_wof_shapes.py
