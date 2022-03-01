#!/usr/bin/env bash

# gdev won't run unless ssh-agent is running.
# If ssh-agent is running, then SSH_AUTH_SOCK will be set and its value will be a socket file.
if [[ ! -S "${SSH_AUTH_SOCK}" ]]; then
    cat <<-'END'
ssh-agent is required for gdev to clone git repositories.
Please start ssh-agent and add your git private keys:
-
    eval `ssh-agent`
    ssh-add
-
END
    exit 1
fi

# --full-trace
PYTEST_ARGS="-ra"
PYTEST_ARGS="$PYTEST_ARGS --cov --cov-branch --cov-report html:report/coverage"
# shellcheck disable=SC2068,SC2086
pipenv run pytest $PYTEST_ARGS $@
