#!/usr/bin/env bash


#enter locations for ssh credential for git repos.
SSH_KEY="$(sudo cat ~/.ssh/id_rsa)"
PYTHON_SCRIPTS=~/PycharmProjects/bi_etl/

sudo docker build \
--build-arg SSH_PRIVATE_KEY="$SSH_KEY" \
--build-arg SCRIPTS=$PYTHON_SCRIPTS \
-t bi_airflow .