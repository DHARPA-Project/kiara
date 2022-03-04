#!/usr/bin/env bash

rm -f ~/.local/share/kiara/kiara.db
rm -fr ~/projects/dharpa/kiara/src/kiara/resources/database/kiara/versions/*
alembic revision --autogenerate --message "Create the example Database"
kiara server reinit-db
datasette ~/.local/share/kiara/kiara.db
