#!/usr/bin/env bash

echo "Removing old db and versions"
rm -f ~/.local/share/kiara/kiara.db
rm -fr ~/projects/kiara_new/kiara/src/kiara/resources/database/kiara/versions/*
echo "Create initial migration version"
alembic revision --autogenerate --message "Create the example Database"
echo "Reinit db"
kiara server reinit-db
echo "Starting datasette"
datasette ~/.local/share/kiara/kiara.db
