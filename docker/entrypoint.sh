#!/bin/bash
#
# Copyright 2018-2023 contributors to the Marquez project
# SPDX-License-Identifier: Apache-2.0
#
# Usage: $ ./entrypoint.sh

set -euo pipefail

if [[ $# -eq 0 ]]; then
  if [[ -z "${MARQUEZ_CONFIG:-}" ]]; then
    MARQUEZ_CONFIG='marquez.dev.yml'
    echo "WARNING 'MARQUEZ_CONFIG' not set, using development configuration."
  fi
  set -- server "${MARQUEZ_CONFIG}"
fi

java_opts=()
if [[ -n "${JAVA_OPTS:-}" ]]; then
  read -r -a java_opts <<< "${JAVA_OPTS}"
fi

exec java \
  "${java_opts[@]}" \
  -Duser.timezone=UTC \
  -Dlog4j2.formatMsgNoLookups=true \
  -jar /usr/src/app/marquez.jar \
  "$@"
