#!/usr/bin/env bash
#
# Copyright 2018-2023 contributors to the Marquez project
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

chart_dir="$(cd "${1:-chart}" && pwd)"
repo_dir="$(dirname "${chart_dir}")"
render_dir="$(mktemp -d)"
trap 'rm -rf "${render_dir}"' EXIT

project_version="$(sed -n 's/^version=//p' "${repo_dir}/gradle.properties")"
chart_version="$(sed -n 's/^version: //p' "${chart_dir}/Chart.yaml")"
chart_app_version="$(sed -n 's/^appVersion: "\(.*\)"$/\1/p' "${chart_dir}/Chart.yaml")"
expected_error="marquez.image.tag is required: chart ${chart_version} configures the durable OpenLineage queue and is incompatible with the 0.51.1 API image; supply an image built from the current source"
api_tag="$(sed -n '/^marquez:/,/^web:/p' "${chart_dir}/values.yaml" | sed -n 's/^    tag: //p' | head -n 1)"
web_tag="$(sed -n '/^web:/,/^postgresql:/p' "${chart_dir}/values.yaml" | sed -n 's/^    tag: //p' | head -n 1)"

if [[ "${chart_version}" != "${project_version}" || "${chart_app_version}" != "${project_version}" ]]; then
  echo "snapshot Chart version/appVersion must match gradle.properties (${project_version})" >&2
  exit 1
fi

if [[ "${api_tag}" != '""' ]]; then
  echo "snapshot chart must fail closed with an empty default marquez.image.tag" >&2
  exit 1
fi

if [[ "${web_tag}" != "0.51.1" ]]; then
  echo "snapshot chart must keep the independently released web image on 0.51.1" >&2
  exit 1
fi

if helm template marquez "${chart_dir}" \
    >"${render_dir}/missing-tag.out" 2>"${render_dir}/missing-tag.err"; then
  echo "expected rendering without marquez.image.tag to fail" >&2
  exit 1
fi

if ! grep -Fq "${expected_error}" "${render_dir}/missing-tag.err"; then
  echo "missing-tag failure did not explain the durable-intake image requirement" >&2
  cat "${render_dir}/missing-tag.err" >&2
  exit 1
fi

helm template marquez "${chart_dir}" \
  --values "${chart_dir}/ci/ci-values.yaml" \
  >"${render_dir}/default.yaml"

helm template marquez "${chart_dir}" \
  --values "${chart_dir}/ci/ci-values.yaml" \
  --show-only templates/marquez/deployment.yaml \
  >"${render_dir}/marquez-deployment.yaml"

helm template marquez "${chart_dir}" \
  --values "${chart_dir}/ci/ci-values.yaml" \
  --set marquez.openLineage.pollIntervalMillis=2000 \
  --set-string marquez.podAnnotations.rollout-test=enabled \
  >"${render_dir}/changed.yaml"

helm template marquez "${chart_dir}" \
  --values "${chart_dir}/ci/ci-values.yaml" \
  --set marquez.db.autoCommentsEnabled=true \
  >"${render_dir}/auto-comments-enabled.yaml"

helm template marquez "${chart_dir}" \
  --values "${chart_dir}/ci/ci-values.yaml" \
  --show-only templates/marquez/configmap.yaml \
  --set marquez.dbRetention.enabled=true \
  --set marquez.dbRetention.frequencyMins=17 \
  --set marquez.dbRetention.numberOfRowsPerBatch=321 \
  --set marquez.dbRetention.retentionDays=29 \
  >"${render_dir}/retention-enabled-configmap.yaml"

if ! grep -Eq '^          image: docker.io/marquezproject/marquez:chart-test$' \
    "${render_dir}/default.yaml"; then
  echo "chart CI values did not render the current-source API image" >&2
  exit 1
fi

if ! grep -Eq '^          imagePullPolicy: Never$' "${render_dir}/default.yaml"; then
  echo "chart CI values did not disable pulling the locally loaded API image" >&2
  exit 1
fi

strategy_count="$(grep -c '^  strategy:$' "${render_dir}/marquez-deployment.yaml" || true)"
recreate_count="$(grep -c '^    type: Recreate$' \
  "${render_dir}/marquez-deployment.yaml" || true)"
if [[ "${strategy_count}" -ne 1 || "${recreate_count}" -ne 1 ]]; then
  echo "Marquez Deployment must use the Recreate strategy" >&2
  exit 1
fi

if grep -Eq '^    type: RollingUpdate$|^    rollingUpdate:$' \
    "${render_dir}/marquez-deployment.yaml"; then
  echo "Marquez Deployment must not overlap old and new projector replicas" >&2
  exit 1
fi

default_checksum="$({ sed -n 's/^        checksum\/marquez-config: "\([^"]*\)"$/\1/p' \
  "${render_dir}/default.yaml" || true; } | head -n 1)"
changed_checksum="$({ sed -n 's/^        checksum\/marquez-config: "\([^"]*\)"$/\1/p' \
  "${render_dir}/changed.yaml" || true; } | head -n 1)"
auto_comments_enabled_checksum="$({ sed -n 's/^        checksum\/marquez-config: "\([^"]*\)"$/\1/p' \
  "${render_dir}/auto-comments-enabled.yaml" || true; } | head -n 1)"

if [[ -z "${default_checksum}" || -z "${changed_checksum}" \
    || -z "${auto_comments_enabled_checksum}" ]]; then
  echo "rendered Deployment is missing checksum/marquez-config on the Pod template" >&2
  exit 1
fi

if [[ "${default_checksum}" == "${changed_checksum}" ]]; then
  echo "ConfigMap checksum did not change with the rendered OpenLineage configuration" >&2
  exit 1
fi

if [[ "${default_checksum}" == "${auto_comments_enabled_checksum}" ]]; then
  echo "ConfigMap checksum did not change when database auto-comments were enabled" >&2
  exit 1
fi

if ! grep -Fq '      autoCommentsEnabled: false' "${render_dir}/default.yaml"; then
  echo "default database auto-comments setting was not rendered as false" >&2
  exit 1
fi

if ! grep -Fq '      autoCommentsEnabled: true' \
    "${render_dir}/auto-comments-enabled.yaml"; then
  echo "database auto-comments true override was not rendered" >&2
  exit 1
fi

if grep -Fxq '    dbRetention:' "${render_dir}/default.yaml"; then
  echo "database retention configuration was rendered while disabled" >&2
  exit 1
fi

for expected_line in \
    '    dbRetention:' \
    '      frequencyMins: 17' \
    '      numberOfRowsPerBatch: 321' \
    '      retentionDays: 29'; do
  if ! grep -Fxq "${expected_line}" \
      "${render_dir}/retention-enabled-configmap.yaml"; then
    echo "enabled database retention configuration was not rendered exactly: ${expected_line}" >&2
    exit 1
  fi
done

if ! grep -Eq '^        rollout-test: "enabled"$' "${render_dir}/changed.yaml"; then
  echo "marquez.podAnnotations was not rendered on the Pod template" >&2
  exit 1
fi

if grep -Eq '^    rollout-test: "enabled"$' "${render_dir}/changed.yaml"; then
  echo "marquez.podAnnotations was rendered on Deployment metadata" >&2
  exit 1
fi

if ! grep -Fq '      pollIntervalMillis: 2000' "${render_dir}/changed.yaml"; then
  echo "OpenLineage override was not rendered into the Marquez ConfigMap" >&2
  exit 1
fi
