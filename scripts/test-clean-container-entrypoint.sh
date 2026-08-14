#!/bin/sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd -P)
entrypoint="$script_dir/test-clean-container.sh"
test_root=$(mktemp -d "${TMPDIR:-/tmp}/tiangong-skills-clean-entrypoint.XXXXXX")
trap 'rm -rf "$test_root"' EXIT HUP INT TERM
fake_bin="$test_root/bin"
mkdir "$fake_bin"

printf '%s\n' \
    '#!/bin/sh' \
    'set -eu' \
    ': "${FAKE_DOCKER_LOG:?}"' \
    'printf '\''%s'\'' "$1" >>"$FAKE_DOCKER_LOG"' \
    'shift' \
    'for argument do' \
    '    printf '\''\t%s'\'' "$argument" >>"$FAKE_DOCKER_LOG"' \
    'done' \
    'printf '\''\n'\'' >>"$FAKE_DOCKER_LOG"' \
    >"$fake_bin/docker"
chmod 700 "$fake_bin/docker"

default_log="$test_root/default.log"
FAKE_DOCKER_LOG="$default_log" PATH="$fake_bin:$PATH" sh "$entrypoint"
default_build=$(sed -n '1p' "$default_log")
case "$default_build" in
    *"$(printf '\t')--no-cache"*)
        printf '%s\n' "default build unexpectedly disabled Docker layer caching" >&2
        exit 1
        ;;
esac
grep -F "$(printf 'run\t--rm')" "$default_log" >/dev/null
grep -F "$(printf '\t--network\tnone')" "$default_log" >/dev/null
grep -F "$(printf '\t--tmpfs\t/home/node:')" "$default_log" >/dev/null

cold_log="$test_root/cold.log"
FAKE_DOCKER_LOG="$cold_log" PATH="$fake_bin:$PATH" sh "$entrypoint" --cold-build
cold_build=$(sed -n '1p' "$cold_log")
case "$cold_build" in
    *"$(printf '\t')--no-cache"*) ;;
    *)
        printf '%s\n' "cold build did not disable Docker layer caching" >&2
        exit 1
        ;;
esac

invalid_log="$test_root/invalid.log"
if FAKE_DOCKER_LOG="$invalid_log" PATH="$fake_bin:$PATH" \
    sh "$entrypoint" --unexpected >/dev/null 2>&1; then
    printf '%s\n' "clean-container entrypoint accepted an unknown mode" >&2
    exit 1
fi
if [ -e "$invalid_log" ]; then
    printf '%s\n' "clean-container entrypoint invoked Docker for an unknown mode" >&2
    exit 1
fi

grep -F 'run: sh scripts/test-clean-container.sh --cold-build' \
    "$repo_root/.github/workflows/docpact.yml" >/dev/null

printf '%s\n' "Skills clean-container entrypoint contract passed."
