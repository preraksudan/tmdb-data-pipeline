#!/bin/bash

mc alias set local http://localhost:9000 minioadmin minioadmin

mc mb local/tmdb-raw || true

mc cp data/raw/tmdb_5000_movies.csv local/tmdb-raw/
mc cp data/raw/tmdb_5000_credits.csv local/tmdb-raw/

mc ls local/tmdb-raw
mc mb local/tmdb-curated